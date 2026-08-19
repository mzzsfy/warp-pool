package warppool

// ID 实例稳定标识(创建序号文本)
type ID string

// Status 实例状态机状态
type Status uint8

const (
	// StatusProbing 探测中:启动或重播后尚未通过出口探测与唯一性校验
	StatusProbing Status = iota
	// StatusNormal 正常:接受新拨号
	StatusNormal
	// StatusDraining 排空中:不接新请求,超时强断在途连接后重播
	StatusDraining
	// StatusDisabled 已禁用:摘除服务,待手动恢复
	StatusDisabled
)

// String 返回状态名
func (s Status) String() string {
	switch s {
	case StatusProbing:
		return "Probing"
	case StatusNormal:
		return "Normal"
	case StatusDraining:
		return "Draining"
	case StatusDisabled:
		return "Disabled"
	}
	return "Unknown"
}
