/** 当前会话未由宿主提供文件能力；读取接口不会自行安装提供者。 */
export class SessionFilesUnavailableError extends Error {
  override readonly name = 'SessionFilesUnavailableError';
  constructor(readonly sessionId: string) { super('REACTIVE_SESSION_FILES_UNAVAILABLE'); }
}

/** 目标或baseFile不在当前会话的工作区范围内。 */
export class SessionFileOutsideWorkspaceError extends Error {
  override readonly name = 'SessionFileOutsideWorkspaceError';
  constructor(readonly path: string) { super('REACTIVE_SESSION_FILE_OUTSIDE_WORKSPACE'); }
}
