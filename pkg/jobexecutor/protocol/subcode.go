package protocol

// DispatchRejected identifies a definite refusal before local admission.
// It uses the default DogX business code and ErrorInfo.Reason. Once admission
// may have occurred, a technical error must not be rewritten to this reason.
const DispatchRejected = "job.executor.dispatch_rejected"
