export interface IRosbagOptions {
  prefetch: number; // default 30 seconds
  playbackSpeed: number; // default 1
  loop: boolean; // default true
}

export interface ITime {
  sec: number;
  nsec: number;
}
export interface IRosbagMessage {
  topic: string;
  time: ITime;
  data: any;
}

export interface IMsgFormat {
  key: string;
  keyType: string;
  isArray: boolean;
  nestedKeys: IMsgFormat[];
  constantValue: undefined | string;
  arrayLength: undefined | number;
}
export interface IMsgSchema {
  topLevelKeys: IMsgFormat[];
  MSGSTypes: Map<string, IMsgFormat>;
}
