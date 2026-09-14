// 带 HTTP 状态码的业务错误
export class HttpError extends Error {
  constructor(status, code, message, extra = {}) {
    super(message);
    this.status = status;
    this.code = code;
    this.extra = extra;
  }
}

export const badRequest = (code, message, extra) =>
  new HttpError(400, code, message, extra);
export const unauthorized = (code = 'UNAUTHENTICATED', message = '未登录') =>
  new HttpError(401, code, message);
export const forbidden = (code = 'FORBIDDEN', message = '没有权限') =>
  new HttpError(403, code, message);
export const conflict = (code, message, extra) =>
  new HttpError(409, code, message, extra);
export const notFound = (code = 'NOT_FOUND', message = '资源不存在') =>
  new HttpError(404, code, message);
