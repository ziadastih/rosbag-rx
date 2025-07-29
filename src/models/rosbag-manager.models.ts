import { Observable } from "rxjs";
import { IRosbagMessage, ITime } from "./general.models";

export interface IBagReader {
  loadFile(file: File): void;
  metadata$: Observable<IBagMetadata>;
  error$: Observable<string>;
  prefetchChunks(startTime: ITime, prefetchVal: number): void;
  getMessagesInRange(start: ITime, end: ITime): IRosbagMessage[];
  destroyReader(): void;
}

export interface IBagMetadata {
  connections: string[];
  startTime: ITime;
  endTime: ITime;
}
