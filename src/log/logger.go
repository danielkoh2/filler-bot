package log

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type LoggerLevel int

const (
	LevelDebug   LoggerLevel = 0
	LevelInfo    LoggerLevel = 1
	LevelWarning LoggerLevel = 2
	LevelError   LoggerLevel = 3
)

type Message struct {
	Channel    string
	LogLevel   LoggerLevel
	LogMessage string
}
type LogChannel struct {
	Name     string
	Level    LoggerLevel
	Disabled bool
	Logger   *Logger
}

type Logger struct {
	LogChannels  map[string]*LogChannel
	Channel      chan Message
	DummyChannel LogChannel
	DefaultLevel LoggerLevel
}

var log *Logger

type LoggerConfig struct {
	Channels     map[string]LoggerLevel
	DefaultLevel LoggerLevel
}

func SetupLogger(config LoggerConfig) {
	logger := &Logger{
		DummyChannel: LogChannel{
			Name:     "default",
			Disabled: false,
		},
		DefaultLevel: config.DefaultLevel,
	}
	logger.LogChannels = make(map[string]*LogChannel)

	logger.Channel = make(chan Message, 10000)
	go func() {
		for logMessage := range logger.Channel {
			logger.writeLog(logMessage)
		}
	}()
	for key, val := range config.Channels {
		logger.LogChannels[key] = &LogChannel{
			Name:     key,
			Level:    val,
			Disabled: false,
			Logger:   logger,
		}
	}
	log = logger
}

func SetLogLevel(level LoggerLevel, channel ...string) {
	channelName := "all"
	if len(channel) > 0 {
		channelName = channel[0]
	}
	if channelName == "all" {
		log.DefaultLevel = level
		for _, logChannel := range log.LogChannels {
			logChannel.Level = level
		}
	} else {
		logChannel, exists := log.LogChannels[channelName]
		if exists {
			logChannel.Level = level
		}
	}
}

func DisableLog(channel ...string) {
	channelName := "all"
	if len(channel) > 0 {
		channelName = channel[0]
	}

	if channelName == "all" {
		for _, logChannel := range log.LogChannels {
			logChannel.Disabled = true
		}
	} else {
		logChannel, exists := log.LogChannels[channelName]
		if exists {
			logChannel.Disabled = true
		}
	}
}

func EnableLog(channel ...string) {
	channelName := "all"
	if len(channel) > 0 {
		channelName = channel[0]
	}

	if channelName == "all" {
		for _, logChannel := range log.LogChannels {
			logChannel.Disabled = false
		}
	} else {
		logChannel, exists := log.LogChannels[channelName]
		if exists {
			logChannel.Disabled = false
		}
	}
}

func (logger *Logger) writeLog(logMessage Message) {
	LoggerChannel, exists := logger.LogChannels[logMessage.Channel]
	if !exists {
		return
	}
	if LoggerChannel.Level > logMessage.LogLevel {
		return
	}

	filename := filepath.Join("logs", time.Now().UTC().Format("20060102"))
	if logMessage.Channel != "default" {
		filename += "_" + logMessage.Channel
	}
	filename += ".log"
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return
	}
	defer func() {
		err = f.Close()
	}()
	_, err = f.WriteString(logMessage.LogMessage)
	if err != nil {
		return
	}
}

func (logger *Logger) write(channel string, level LoggerLevel, msg string, params ...any) {
	if len(params) > 0 {
		msg = fmt.Sprintf(msg, params...)
	}
	levelString := "[DEBUG]"
	switch level {
	case LevelInfo:
		levelString = "[INFO]"
	case LevelWarning:
		levelString = "[WARNING]"
	case LevelError:
		levelString = "[ERROR]"
	}
	if channel != "telegram" {
		if testing.Testing() {
			msg = fmt.Sprintf("[%s],%s,[%s],%s\n", time.Now().Format(time.RFC3339Nano), levelString, channel, msg)
		} else {
			msg = fmt.Sprintf("[%s],%s,%s\n", time.Now().Format(time.RFC3339Nano), levelString, msg)
		}
	}
	if testing.Testing() {
		fmt.Print(msg)
	} else {
		//fmt.Println(msg)
		logger.Channel <- Message{
			Channel:    channel,
			LogMessage: msg,
			LogLevel:   level,
		}
	}
}
func Debug(msg string, params ...any) {
	log.write("default", LevelDebug, msg, params...)
}

func Info(msg string, params ...any) {
	log.write("default", LevelInfo, msg, params...)
}

func Warning(msg string, params ...any) {
	log.write("default", LevelWarning, msg, params...)
}

func Error(msg string, params ...any) {
	log.write("default", LevelError, msg, params...)
}

func Channel(channel string, level ...LoggerLevel) *LogChannel {
	val, exists := log.LogChannels[channel]
	if !exists {
		loggerLevel := log.DefaultLevel
		if len(level) > 0 {
			loggerLevel = level[0]
		}
		loggerChannel := &LogChannel{
			Name:     channel,
			Level:    loggerLevel,
			Disabled: false,
			Logger:   log,
		}
		log.LogChannels[channel] = loggerChannel
		return loggerChannel
	}
	return val
}

func (channel *LogChannel) Debug(msg string, params ...any) {
	if channel.Disabled {
		return
	}
	channel.Logger.write(channel.Name, LevelDebug, msg, params...)
}

func (channel *LogChannel) Info(msg string, params ...any) {
	if channel.Disabled {
		return
	}
	channel.Logger.write(channel.Name, LevelInfo, msg, params...)
}

func (channel *LogChannel) Warning(msg string, params ...any) {
	if channel.Disabled {
		return
	}
	channel.Logger.write(channel.Name, LevelWarning, msg, params...)
}

func (channel *LogChannel) Error(msg string, params ...any) {
	if channel.Disabled {
		return
	}
	channel.Logger.write(channel.Name, LevelError, msg, params...)
}
