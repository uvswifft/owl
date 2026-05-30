package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gowvp/owl/internal/conf"
	"github.com/gowvp/owl/internal/core/event"
	"github.com/gowvp/owl/internal/core/ipc"
	"github.com/gowvp/owl/internal/core/sms"
	"github.com/gowvp/owl/internal/rpc"
	"github.com/gowvp/owl/protos"
	"github.com/ixugo/goddd/pkg/conc"
	"github.com/ixugo/goddd/pkg/orm"
	"github.com/ixugo/goddd/pkg/system"
	"github.com/ixugo/goddd/pkg/web"
)

// AIWebhookAPI 处理 AI 分析服务的回调请求
type AIWebhookAPI struct {
	log       *slog.Logger
	conf      *conf.Bootstrap
	aiTasks   *conc.Map[string, struct{}]
	limiter   func(identifier string) bool
	ai        *rpc.AIClient
	eventCore event.Core
	ipcCore   ipc.Core
}

// NewAIWebhookAPI 创建 AI Webhook API 实例
func NewAIWebhookAPI(conf *conf.Bootstrap, eventCore event.Core, ipcCore ipc.Core) AIWebhookAPI {
	return AIWebhookAPI{
		log:       slog.With("hook", "ai"),
		conf:      conf,
		ai:        rpc.NewAIClient("127.0.0.1:50051"),
		aiTasks:   conc.NewMap[string, struct{}](),
		eventCore: eventCore,
		ipcCore:   ipcCore,
		limiter:   web.IDRateLimiter(0.2, 1, 3*time.Minute),
	}
}

// registerAIWebhookAPI 注册 AI 回调路由，接收来自 Python AI 服务的各类事件通知
func registerAIWebhookAPI(r gin.IRouter, api AIWebhookAPI, handler ...gin.HandlerFunc) {
	group := r.Group("/ai", handler...)
	group.POST("/keepalive", web.WrapH(api.onKeepalive))
	group.POST("/started", web.WrapH(api.onStarted))
	group.POST("/events", web.WrapH(api.onEvents))
	group.POST("/stopped", web.WrapH(api.onStopped))
}

// onKeepalive 接收 AI 服务心跳，用于监控 AI 服务存活状态
func (a AIWebhookAPI) onKeepalive(c *gin.Context, in *AIKeepaliveInput) (AIWebhookOutput, error) {
	var activeStreams int
	var uptimeSeconds int64
	if in.Stats != nil {
		activeStreams = in.Stats.ActiveStreams
		uptimeSeconds = in.Stats.UptimeSeconds
	}
	a.log.InfoContext(c.Request.Context(), "ai keepalive",
		"timestamp", in.Timestamp,
		"message", in.Message,
		"active_streams", activeStreams,
		"uptime_seconds", uptimeSeconds,
	)
	return newAIWebhookOutputOK(), nil
}

// onStarted 接收 AI 服务启动通知，确认 AI 服务已就绪
func (a AIWebhookAPI) onStarted(c *gin.Context, in *AIStartedInput) (AIWebhookOutput, error) {
	a.log.InfoContext(c.Request.Context(), "ai started",
		"timestamp", in.Timestamp,
		"message", in.Message,
	)
	return newAIWebhookOutputOK(), nil
}

// onEvents 接收 AI 检测事件，按 label 分别存储到数据库，图片保存到 configs/events 目录
func (a AIWebhookAPI) onEvents(c *gin.Context, in *AIDetectionInput) (AIWebhookOutput, error) {
	if !a.limiter(in.CameraID) {
		return newAIWebhookOutputOK(), nil
	}
	ctx := c.Request.Context()

	a.log.InfoContext(ctx, "ai detection event",
		"camera_id", in.CameraID,
		"timestamp", in.Timestamp,
		"detection_count", len(in.Detections),
		"snapshot_size", fmt.Sprintf("%dx%d", in.SnapshotWidth, in.SnapshotHeight),
	)

	// 获取通道信息以确定 DID
	cid := in.CameraID
	var did string
	channel, err := a.ipcCore.GetChannel(ctx, cid)
	if err == nil && channel != nil {
		did = channel.DID
	}

	// 保存图片并获取相对路径
	var imagePath string
	if in.Snapshot != "" {
		var err error
		imagePath, err = saveEventSnapshot(cid, in.Timestamp, in.Snapshot)
		if err != nil {
			a.log.ErrorContext(ctx, "save snapshot failed", "err", err)
		}
	}

	// 按 label 分别存储事件，每个 label 是一个独立事件
	for i, det := range in.Detections {
		a.log.InfoContext(ctx, "detection detail",
			"index", i,
			"label", det.Label,
			"confidence", det.Confidence,
			"box", fmt.Sprintf("(%d,%d)-(%d,%d)", det.Box.XMin, det.Box.YMin, det.Box.XMax, det.Box.YMax),
			"area", det.Area,
		)

		zonesJSON, _ := json.Marshal(det.Box)

		eventInput := &event.AddEventInput{
			DID:       did,
			CID:       cid,
			StartedAt: in.Timestamp,
			EndedAt:   in.Timestamp,
			Label:     det.Label,
			Score:     float32(det.Confidence),
			Zones:     string(zonesJSON),
			ImagePath: imagePath,
			// TODO: 模型名称可以根据模型自定义
			Model: "default",
		}

		if _, err := a.eventCore.AddEvent(ctx, eventInput); err != nil {
			a.log.ErrorContext(ctx, "save event failed",
				"label", det.Label,
				"err", err,
			)
		}
	}

	return newAIWebhookOutputOK(), nil
}

// onStopped 接收 AI 任务停止通知，记录停止原因
func (a AIWebhookAPI) onStopped(c *gin.Context, in *AIStoppedInput) (AIWebhookOutput, error) {
	a.log.InfoContext(c.Request.Context(), "ai task stopped",
		"camera_id", in.CameraID,
		"timestamp", in.Timestamp,
		"reason", in.Reason,
		"message", in.Message,
	)
	a.aiTasks.Delete(in.CameraID)
	return newAIWebhookOutputOK(), nil
}

// StartAISyncLoop 启动 AI 任务同步协程，启动后立即执行一次同步，之后每 5 分钟检测一次
// 立即执行是为了在服务重启后尽快恢复之前开启的 AI 分析任务
func (a *AIWebhookAPI) StartAISyncLoop(ctx context.Context, smsCore sms.Core) {
	go func() {
		// 延迟 30 秒再首次同步，等待设备注册和 catalog 更新完成，避免读到过期状态
		select {
		case <-ctx.Done():
			return
		case <-time.After(30 * time.Second):
			a.syncAITasks(ctx, smsCore)
		}

		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				a.log.Info("AI sync loop stopped")
				return
			case <-ticker.C:
				a.syncAITasks(ctx, smsCore)
			}
		}
	}()
}

// syncAITasks 同步 AI 任务状态，确保数据库中 enabled_ai=true 的通道正在运行检测，enabled_ai=false 的已停止
func (a *AIWebhookAPI) syncAITasks(ctx context.Context, smsCore sms.Core) {
	if a.conf.Server.AI.Disabled || a.ai == nil {
		return
	}

	// 查询所有在线通道
	channels, _, err := a.ipcCore.FindChannel(ctx, &ipc.FindChannelInput{
		PagerFilter: web.PagerFilter{Page: 1, Size: 999},
		IsOnline:    "true",
	})
	if err != nil {
		a.log.ErrorContext(ctx, "sync ai tasks: find channels failed", "err", err)
		return
	}

	// 构建数据库中 enabled_ai=true 的通道集合
	dbEnabledSet := make(map[string]*ipc.Channel)
	for _, ch := range channels {
		if ch.Ext.EnabledAI {
			dbEnabledSet[ch.ID] = ch
		}
	}

	// 收集内存中正在运行的任务
	memoryTasks := make(map[string]struct{})
	a.aiTasks.Range(func(key string, _ struct{}) bool {
		memoryTasks[key] = struct{}{}
		return true
	})

	// 需要启动的任务：数据库中 enabled 但内存中没有
	for channelID, ch := range dbEnabledSet {
		if _, exists := memoryTasks[channelID]; !exists {
			a.log.Info("sync: starting AI task", "channel_id", channelID)
			if err := a.startAITask(ctx, smsCore, ch); err != nil {
				a.log.ErrorContext(ctx, "sync: start AI task failed", "channel_id", channelID, "err", err)
			}
		}
	}

	// 需要停止的任务：内存中有但数据库中 disabled
	for channelID := range memoryTasks {
		if _, exists := dbEnabledSet[channelID]; !exists {
			a.log.Info("sync: stopping AI task", "channel_id", channelID)
			if err := a.stopAITask(ctx, channelID); err != nil {
				a.log.ErrorContext(ctx, "sync: stop AI task failed", "channel_id", channelID, "err", err)
			}
		}
	}
}

// startAITask 启动单个通道的 AI 检测任务（内部使用，自动构建 RTSP URL）
func (a *AIWebhookAPI) startAITask(ctx context.Context, smsCore sms.Core, ch *ipc.Channel) error {
	svr, err := smsCore.GetMediaServer(ctx, sms.DefaultMediaServerID)
	if err != nil {
		return fmt.Errorf("get media server: %w", err)
	}
	rtspURL := fmt.Sprintf("rtsp://127.0.0.1:%d/rtp/%s", svr.Ports.RTSP, ch.ID)
	_, err = a.StartAIDetection(ctx, ch, rtspURL)
	return err
}

// stopAITask 停止单个通道的 AI 检测任务（内部使用）
func (a *AIWebhookAPI) stopAITask(ctx context.Context, channelID string) error {
	return a.StopAIDetection(ctx, channelID)
}

// StartAIDetection 启动 AI 检测任务，供外部调用（如 ipc.go 中的 enableAI）
func (a *AIWebhookAPI) StartAIDetection(ctx context.Context, ch *ipc.Channel, rtspURL string) (*protos.StartCameraResponse, error) {
	if a.ai == nil {
		return nil, fmt.Errorf("AI service not initialized")
	}

	roiPoints, labels := a.extractZoneConfig(ch)

	// 三级回退：通道自定义 → 配置文件全局默认 → 内置兜底 5 秒
	interval := ch.Ext.AnalysisInterval
	if interval <= 0 {
		interval = a.conf.Server.AI.AnalysisInterval
	}
	if interval <= 0 {
		interval = 5.0
	}
	resp, err := a.ai.StartCamera(ctx, &protos.StartCameraRequest{
		CameraId:              ch.ID,
		CameraName:            ch.Name,
		RtspUrl:               rtspURL,
		DetectIntervalSeconds: interval,
		Labels:                labels,
		Threshold:             0.75,
		RoiPoints:             roiPoints,
		RetryLimit:            10,
		CallbackUrl:           fmt.Sprintf("http://127.0.0.1:%d/ai", a.conf.Server.HTTP.Port),
		CallbackSecret:        "Basic 1234567890",
	})
	if err != nil {
		return nil, err
	}

	a.aiTasks.Store(ch.ID, struct{}{})
	return resp, nil
}

// StopAIDetection 停止 AI 检测任务，供外部调用（如 ipc.go 中的 disableAI）
func (a *AIWebhookAPI) StopAIDetection(ctx context.Context, channelID string) error {
	if a.ai == nil {
		return nil
	}
	_, err := a.ai.StopCamera(ctx, &protos.StopCameraRequest{
		CameraId: channelID,
	})
	// 无论是否成功都从内存中删除，避免重复尝试停止已不存在的任务
	a.aiTasks.Delete(channelID)
	return err
}

// extractZoneConfig 从通道配置中提取区域和标签信息
func (a *AIWebhookAPI) extractZoneConfig(ch *ipc.Channel) (roiPoints []float32, labels []string) {
	if len(ch.Ext.Zones) > 0 {
		zone := ch.Ext.Zones[0]
		roiPoints = zone.Coordinates
		labels = zone.Labels
	}
	if len(labels) == 0 {
		labels = []string{"person", "car", "cat", "dog"}
	}
	return
}

// saveEventSnapshot 将 Base64 编码的快照保存到 configs/events/{cid}/ 目录
// 返回相对路径: cid/年月日时分秒_随机6位.jpg
func saveEventSnapshot(cid string, t orm.Time, snapshotB64 string) (string, error) {
	eventsDir := filepath.Join(system.Getwd(), "configs", "events")

	data, err := base64.StdEncoding.DecodeString(snapshotB64)
	if err != nil {
		return "", fmt.Errorf("decode base64: %w", err)
	}

	randomSuffix := fmt.Sprintf("%06d", rand.IntN(1000000))
	filename := fmt.Sprintf("%s_%s.jpg", t.Format("20060102150405"), randomSuffix)

	relativePath := filepath.Join(cid, filename)
	fullPath := filepath.Join(eventsDir, relativePath)

	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		return "", fmt.Errorf("create events dir: %w", err)
	}

	if err := os.WriteFile(fullPath, data, 0o644); err != nil {
		return "", fmt.Errorf("write file: %w", err)
	}

	slog.Info("event snapshot saved", "path", fullPath, "size", len(data))
	return relativePath, nil
}
