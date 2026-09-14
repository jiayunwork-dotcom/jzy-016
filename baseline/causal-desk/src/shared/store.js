import { promises as fs } from 'node:fs';
import path from 'node:path';

// 原子 JSON 存储：所有变更先改内存副本，最后一次性 writeTmp + rename 落盘。
// mutate(fn) 提供"整批要么全进要么整批退"的事务边界：
//   fn 内抛错 => 内存状态保持原样、绝不落盘，整批退回。
export class JsonStore {
  constructor(file, initial) {
    this.file = path.resolve(file);
    this.data = structuredClone(initial);
    this._writing = Promise.resolve();
  }

  async load() {
    await fs.mkdir(path.dirname(this.file), { recursive: true });
    try {
      const raw = await fs.readFile(this.file, 'utf8');
      this.data = JSON.parse(raw);
    } catch (e) {
      if (e.code !== 'ENOENT') throw e;
      await this.persist();
    }
    return this.data;
  }

  async persist() {
    await fs.mkdir(path.dirname(this.file), { recursive: true });
    const tmp = `${this.file}.${process.pid}.${Date.now()}.tmp`;
    const payload = JSON.stringify(this.data, null, 2);
    // 串行化落盘，避免 rename 互相覆盖
    const next = this._writing.then(async () => {
      await fs.writeFile(tmp, payload, 'utf8');
      await fs.rename(tmp, this.file);
    });
    this._writing = next.catch(() => {});
    return next;
  }

  // 读快照（返回内部引用；调用方只读）
  read() {
    return this.data;
  }

  // 事务：在深副本上执行 fn；成功则整体替换并落盘，失败则什么都不发生
  async mutate(fn) {
    const draft = structuredClone(this.data);
    const result = await fn(draft);
    this.data = draft;
    await this.persist();
    return result;
  }
}
