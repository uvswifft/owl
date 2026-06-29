package zlm

const (
	startRecordPath  = "/index/api/startRecord"
	stopRecordPath   = "/index/api/stopRecord"
	getMediaListPath = "/index/api/getMediaList"
)

// StartRecordRequest 开始录制请求参数
type StartRecordRequest struct {
	Type       int    `json:"type"`                 // 0 为 hls，1 为 mp4
	Vhost      string `json:"vhost"`                // 虚拟主机
	App        string `json:"app"`                  // 应用名
	Stream     string `json:"stream"`               // 流 ID
	CustomPath string `json:"customized_path"`      // 自定义存储路径
	MaxSecond  int    `json:"max_second,omitempty"` // 录制时长，单位秒，置 0 则不限制
}

// StartRecordResponse 开始录制响应
type StartRecordResponse struct {
	FixedHeader
	Result bool `json:"result"` // 是否成功
}

// StopRecordRequest 停止录制请求参数
type StopRecordRequest struct {
	Type   int    `json:"type"`   // 0 为 hls，1 为 mp4
	Vhost  string `json:"vhost"`  // 虚拟主机
	App    string `json:"app"`    // 应用名
	Stream string `json:"stream"` // 流 ID
}

// StopRecordResponse 停止录制响应
type StopRecordResponse struct {
	FixedHeader
	Result bool `json:"result"` // 是否成功
}

// StartRecord 开始录制，触发 ZLM 对指定流进行 MP4 录制
func (e *Engine) StartRecord(req StartRecordRequest) (*StartRecordResponse, error) {
	data := map[string]any{
		"type":   req.Type,
		"vhost":  req.Vhost,
		"app":    req.App,
		"stream": req.Stream,
	}
	if req.CustomPath != "" {
		data["customized_path"] = req.CustomPath
	}
	if req.MaxSecond > 0 {
		data["max_second"] = req.MaxSecond
	}

	var resp StartRecordResponse
	if err := e.post(startRecordPath, data, &resp); err != nil {
		return nil, err
	}
	if err := e.ErrHandle(resp.Code, resp.Msg); err != nil {
		return nil, err
	}
	return &resp, nil
}

// StopRecord 停止录制
func (e *Engine) StopRecord(req StopRecordRequest) (*StopRecordResponse, error) {
	data := map[string]any{
		"type":   req.Type,
		"vhost":  req.Vhost,
		"app":    req.App,
		"stream": req.Stream,
	}

	var resp StopRecordResponse
	if err := e.post(stopRecordPath, data, &resp); err != nil {
		return nil, err
	}
	if err := e.ErrHandle(resp.Code, resp.Msg); err != nil {
		return nil, err
	}
	return &resp, nil
}

// MediaItem getMediaList 返回的单条流信息
type MediaItem struct {
	App            string `json:"app"`
	Stream         string `json:"stream"`
	Schema         string `json:"schema"`
	Vhost          string `json:"vhost"`
	IsRecordingMP4 bool   `json:"isRecordingMP4"`
	IsRecordingHLS bool   `json:"isRecordingHLS"`
	OriginType     int    `json:"originType"`
	ReaderCount    int    `json:"readerCount"`
	AliveSecond    int    `json:"aliveSecond"`
}

// GetMediaListResponse getMediaList 响应
type GetMediaListResponse struct {
	FixedHeader
	Data []MediaItem `json:"data"`
}

// GetMediaList 批量获取所有在线流列表（含录制状态）
// 一次请求获取全部流的 isRecordingMP4 状态，避免逐流查询
func (e *Engine) GetMediaList() (*GetMediaListResponse, error) {
	var resp GetMediaListResponse
	if err := e.post(getMediaListPath, nil, &resp); err != nil {
		return nil, err
	}
	if err := e.ErrHandle(resp.Code, resp.Msg); err != nil {
		return nil, err
	}
	return &resp, nil
}
