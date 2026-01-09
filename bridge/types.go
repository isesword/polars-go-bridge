package bridge

// ErrorCode 错误码
type ErrorCode int32

const (
	ErrOK                     ErrorCode = 0
	ErrUnknown                ErrorCode = 1
	ErrInvalidArgument        ErrorCode = 2
	ErrAbiMismatch            ErrorCode = 3
	ErrPlanVersionUnsupported ErrorCode = 4
	ErrPlanDecode             ErrorCode = 5
	ErrPlanSemantic           ErrorCode = 6
	ErrArrowImport            ErrorCode = 7
	ErrArrowExport            ErrorCode = 8
	ErrExecution              ErrorCode = 9
	ErrUnsupported            ErrorCode = 10
	ErrOom                    ErrorCode = 11
)
