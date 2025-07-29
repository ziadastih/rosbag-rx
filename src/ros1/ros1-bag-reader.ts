import {
  BehaviorSubject,
  filter,
  from,
  map,
  mergeMap,
  Observable,
  Subject,
  takeUntil,
} from "rxjs";
import { IBagMetadata, IBagReader } from "../models/rosbag-manager.models";
import { Ros1BagInspector } from "./ros1-bag-inspector";
import { IRos1BagMetadata } from "./models/ros1-bag-inspector.model";
import { addSecToTime, compareTime } from "../utils/timeUtil";
import { Ros1ChunkManager } from "./ros1-chunk-manager";
import { IRosbagMessage, ITime } from "../models/general.models";

export class Ros1BagReader implements IBagReader {
  private _bagInspector = new Ros1BagInspector();
  private _chunkManager = new Ros1ChunkManager();
  private _bagMetadata$ = new BehaviorSubject<IRos1BagMetadata | null>(null);
  private _cancelPrefetch$ = new Subject<void>();
  private _destroyInstance$ = new Subject<void>();
  constructor() {
    this._bagInspector.bagMetadata$.subscribe((res) => {
      this._bagMetadata$.next(res);
    });
  }

  get metadata$(): Observable<IBagMetadata> {
    return this._bagMetadata$.pipe(
      filter((res) => !!res),
      map((res) => {
        return {
          connections: Array.from(res.connections).map(([key, value]) => {
            return value.topicName;
          }),
          startTime: res.startTime,
          endTime: res.endTime,
        };
      }),
      takeUntil(this._destroyInstance$)
    );
  }

  get error$(): Observable<string> {
    return this._bagInspector.error$;
  }

  loadFile(file: File): void {
    this._bagInspector.setFile(file);
    this._chunkManager.setFile(file);
  }

  prefetchChunks(startTime: ITime, prefetchVal: number): void {
    if (!this._bagMetadata$.value) return;
    const prefetchEndTime = addSecToTime(startTime, prefetchVal);
    const { chunksInfo, endTime } = this._bagMetadata$.value;
    const validEndTime =
      compareTime(prefetchEndTime, endTime) > 0 ? endTime : prefetchEndTime;

    const relevantChunks = chunksInfo.filter((chunk) => {
      return (
        compareTime(chunk.endTime, startTime) >= 0 &&
        compareTime(chunk.startTime, validEndTime) <= 0
      );
    });

    from(relevantChunks)
      .pipe(
        filter((chunk) => !this._chunkManager.hasChunk(chunk.idx)),
        mergeMap(
          (chunk) =>
            this._chunkManager.readChunk$(
              chunk,
              chunk.nextChunkPosition,
              this._bagMetadata$.value.connections,
              this._cancelPrefetch$
            ),
          2
        ),
        takeUntil(this._cancelPrefetch$)
      )
      .subscribe();
  }

  getMessagesInRange(start: ITime, end: ITime): IRosbagMessage[] {
    const { chunksInfo } = this._bagMetadata$.value ?? {};
    if (!chunksInfo) return [];
    const relevantChunks = chunksInfo.filter((chunk) => {
      return (
        compareTime(chunk.endTime, start) >= 0 &&
        compareTime(chunk.startTime, end) <= 0
      );
    });

    const result: IRosbagMessage[] = [];
    for (let i = 0; i < relevantChunks.length; i++) {
      if (!this._chunkManager.hasChunk(relevantChunks[i].idx)) continue;
      const cached = this._chunkManager.getCachedChunk(relevantChunks[i].idx);
      for (let j = 0; j < cached.length; j++) {
        if (
          compareTime(cached[j].time, start) >= 0 &&
          compareTime(cached[j].time, end) <= 0
        ) {
          result.push(cached[j]);
        }
      }
    }

    return result;
  }

  destroyReader(): void {
    this._cancelPrefetch$.next();
    this._cancelPrefetch$.complete();
    this._destroyInstance$.next();
    this._destroyInstance$.complete();
    this._bagInspector.destroyInstance();
  }
}
