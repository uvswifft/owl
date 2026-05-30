package conf

import (
	"time"

	"github.com/ixugo/goddd/pkg/orm"
)

func DefaultConfig() Bootstrap {
	return Bootstrap{
		Server: Server{
			Username:   "admin",
			Password:   "admin",
			RTMPSecret: "123",
			HTTP: ServerHTTP{
				Port:      15123,
				Timeout:   Duration(60 * time.Second),
				JwtSecret: orm.GenerateRandomString(24),
				AuthURL:   "",
				PProf: ServerPPROF{
					Enabled:   true,
					AccessIps: []string{"::1", "127.0.0.1"},
				},
			},
			AI: ServerAI{
				Disabled:         false,
				RetainDays:       7,
				AnalysisInterval: 5.0,
			},
			Recording: ServerRecording{
				Disabled:           false,
				StorageDir:         "./configs/recordings",
				RetainDays:         3,
				DiskUsageThreshold: 95.0,
				SegmentSeconds:     300,
			},
		},
		Data: Data{
			Database: Database{
				Dsn:             "./configs/data.db",
				MaxIdleConns:    10,
				MaxOpenConns:    50,
				ConnMaxLifetime: Duration(6 * time.Hour),
				SlowThreshold:   Duration(200 * time.Millisecond),
			},
		},
		Sip: SIP{
			Port:     15060,
			ID:       "34010000002000000001",
			Password: "",
		},
		Media: Media{
			IP:           "127.0.0.1",
			HTTPPort:     80,
			Secret:       "",
			WebHookIP:    "127.0.0.1",
			SDPIP:        "127.0.0.1",
			RTPPortRange: "20000-20100",
			Type:         "zlm",
		},
		Log: Log{
			Dir:          "./logs",
			Level:        "error",
			MaxAge:       Duration(3 * 24 * time.Hour),
			RotationTime: Duration(8 * time.Hour),
			RotationSize: 50,
		},
	}
}
