import crypto from 'node:crypto';
import { unauthorized, forbidden } from './errors.js';

// ---- 密码：scrypt ----
export async function hashPassword(password, salt = crypto.randomBytes(16).toString('hex')) {
  const dk = await new Promise((resolve, reject) =>
    crypto.scrypt(password, salt, 64, (err, d) => (err ? reject(err) : resolve(d)))
  );
  return { salt, hash: dk.toString('hex') };
}

export async function verifyPassword(password, rec) {
  const { hash } = await hashPassword(password, rec.salt);
  // 等长比较，避免提前返回
  const a = Buffer.from(hash, 'hex');
  const b = Buffer.from(rec.hash, 'hex');
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

// ---- JWT (HS256)，不引第三方 ----
function b64url(obj) {
  return Buffer.from(JSON.stringify(obj)).toString('base64url');
}
function sign(data, secret) {
  return crypto.createHmac('sha256', secret).update(data).digest('base64url');
}

export function issueToken(user, secret, ttlSec = 8 * 3600) {
  const header = b64url({ alg: 'HS256', typ: 'JWT' });
  const now = Math.floor(Date.now() / 1000);
  const payload = b64url({ sub: user.id, role: user.role, name: user.name, iat: now, exp: now + ttlSec });
  const sig = sign(`${header}.${payload}`, secret);
  return `${header}.${payload}.${sig}`;
}

export function verifyToken(token, secret) {
  if (!token || typeof token !== 'string') throw unauthorized();
  const parts = token.split('.');
  if (parts.length !== 3) throw unauthorized('BAD_TOKEN', '令牌格式错误');
  const [header, payload, sig] = parts;
  const expect = sign(`${header}.${payload}`, secret);
  const a = Buffer.from(sig);
  const b = Buffer.from(expect);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    throw unauthorized('BAD_SIGNATURE', '签名无效');
  }
  let claims;
  try {
    claims = JSON.parse(Buffer.from(payload, 'base64url').toString('utf8'));
  } catch {
    throw unauthorized('BAD_TOKEN', '令牌无法解析');
  }
  if (claims.exp && Date.now() / 1000 > claims.exp) {
    throw unauthorized('TOKEN_EXPIRED', '登录已过期');
  }
  return claims;
}

// ---- Express 中间件 ----
// 没登录谁也打不开流：所有 /api 数据接口都挂 requireAuth
export function requireAuth(secret) {
  return (req, _res, next) => {

    const header = req.headers.authorization || '';
    const token = header.startsWith('Bearer ') ? header.slice(7) : null;
    try {
      req.user = verifyToken(token, secret);
      next();
    } catch (e) {
      next(e);
    }
  };
}

// 工位隔离：落库工(ingest)/消费工(consume)/对账工(reconcile) 越权一律 403，
// 页面入口拦住，后台同样拒绝。
export function requireRole(...roles) {
  return (req, _res, next) => {
    if (!req.user) return next(unauthorized());
    if (!roles.includes(req.user.role)) {
      return next(
        forbidden(
          'ROLE_DENIED',
          `「${roleLabel(req.user.role)}」无权执行此操作（仅 ${roles.map(roleLabel).join('/')} 可执行）`
        )
      );
    }
    next();
  };
}

export function roleLabel(role) {
  return { ingest: '落库工', consume: '消费工', reconcile: '对账工' }[role] || role;
}
