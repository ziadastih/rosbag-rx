export type CallbackFn<T> = (value: T) => void;

export interface IRosbagOptions {
  prefetch: number; // default 30 seconds
  playbackSpeed: number; // default 1
  loop: boolean; // default true
}

export interface IRecord {
  recordOffset: number;
  recordLength: number;
  recordDataOffset: number;
}

export interface IRecordFields {
  [key: string]: Buffer;
}

export interface IRecordShallow extends IRecord {
  recordHeaderFields: IRecordFields;
  recordDataBuffer: Buffer;
}

export interface ITime {
  sec: number;
  nsec: number;
}
