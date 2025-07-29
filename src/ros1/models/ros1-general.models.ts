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
