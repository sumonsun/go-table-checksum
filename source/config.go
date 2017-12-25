package tablechecksum

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

type Config struct {
	Chunksize   int                  `toml:"chunksize"`
	Goroutine   int                  `toml:"goroutine"`
	LogConfig   *LogConfig           `toml:"log"`
	Mysqlsource *[]MysqlsourceConfig `toml:"mysqlsource"`
	Mysqlaims   *[]MysqlaimsConfig   `toml:"mysqlaims"`
	Checkinfo   *Checkinfo           `toml:"checkinfo"`
}
type LogConfig struct {
	Path         string `toml:"log_path"`
	Type         int    `toml:"type"`
	Highlighting bool   `toml:"highlighting"`
	Level        string `toml:"level"`
}
type MysqlsourceConfig struct {
	Addr     string `toml:"addr"`
	Port     string `toml:"port"`
	User     string `toml:"user"`
	Password string `toml:"password"`
}

type MysqlaimsConfig struct {
	Addr     string `toml:"addr"`
	Port     string `toml:"port"`
	User     string `toml:"user"`
	Password string `toml:"password"`
}

type Checkinfo struct {
	Database string `toml:"database"`
	Table    string `toml:"table"`
}

//NewConfigWithFile 读取配置文件
func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewConfig(string(data))
}

//NewConfig 解析配置文件
func NewConfig(data string) (*Config, error) {
	var c Config

	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &c, nil
}
