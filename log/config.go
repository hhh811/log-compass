package log

type LogConf struct {
	Level string `json:",default=info,options[debug,info,error]`
}
