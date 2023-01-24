// package config contains project config types
// and function to read config file
package config

import (
	"bulker/internal/rabbitmq"
	"github.com/spf13/viper"
)

// Database config
type DBConfig struct {
	Database string
	User     string
	Password string
	Host     string
}

// Files config (Log, PID)
type FileConfig struct {
	LogFile string
	PidFile string
}

// Aliases
type Alias struct {
	Key   string
	Alias string
}

// Common structure, use it for object creation
type Common struct {
	Obj    map[string]interface{}
	Config Config
	Csend  chan map[string]interface{}
}

// Common config structure
type Config struct {
	DB       DBConfig
	RabbitMQ rabbitmq.RabbitMQConfig
	Global   GlobalConfig
	Bulk     BulkConfig
}

// Global project config
type GlobalConfig struct {
	Free_number string
	Test_mode   bool
}

// Bulker config
type BulkConfig struct {
	File          FileConfig
	Interval      int
	Max_idle_conn int
	Consume_queue string
	File_path     string
	Msisdn_regexp string
	Msisdn_length int
}

const BasePath string = "/1/www/cmssite/quiz_megafon_ru"

// ReadConfig - read and parse config file in YAML format
func ReadConfig(cfile string, cfg interface{}, alias ...Alias) (err error) {

	v := viper.New()
	v.SetConfigFile(cfile)

	for _, val := range alias {
		v.RegisterAlias(val.Key, val.Alias)
	}

	if err = v.ReadInConfig(); err != nil {
		return err
	}

	err = v.Unmarshal(cfg)

	return err
} // ReadConfig
