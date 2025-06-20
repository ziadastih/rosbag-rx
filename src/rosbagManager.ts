import {
  BehaviorSubject,
  combineLatest,
  exhaustMap,
  filter,
  from,
  interval,
  map,
  mergeMap,
  of,
  shareReplay,
  Subject,
  takeUntil,
  tap,
} from "rxjs";
import { BagInspector } from "./bag-inspector";
import { ChunkInfoManager } from "./chunk-info-manager";
import { IBagMetadata } from "./models/bag-inspector.model";
import { addSecToTime, compareTime, isLessThan } from "./utils/timeUtil";
import { IRosbagOptions, ITime } from "./models/general.models";
import { IRosbagMessage } from "./models/chunk-info-manager.model";

export class RosbagManager {
  private _bagInspector: BagInspector;
  private _bagMetadata$ = new BehaviorSubject<IBagMetadata | null>(null);
  private _chunkManager: ChunkInfoManager;
  private _options$ = new BehaviorSubject<IRosbagOptions>({
    prefetch: 10,
    playbackSpeed: 1,
    loop: true,
  });
  private _isPlaying$ = new BehaviorSubject<boolean>(false);
  private _currentBagTime$ = new BehaviorSubject<ITime | null>(null);
  private _wallStartTime = 0;
  private _playbackInterval$ = interval(33);
  private _onMessages = new Subject<IRosbagMessage[]>();
  private _destroyInstance$ = new Subject<void>();
  private _seek$ = new Subject<{ time: ITime; autoResume: boolean }>();
  private _cancelPrefetch$ = new Subject<void>();
  constructor() {
    this._bagInspector = new BagInspector();
    this._chunkManager = new ChunkInfoManager();
    this._bagInspector.bagMetadata$.subscribe((res) => {
      this._bagMetadata$.next(res);
      this._currentBagTime$.next(res.startTime);
      this._prefetchChunks(res.startTime);
    });
    this._seek$
      .pipe(
        takeUntil(this._destroyInstance$),
        tap(() => this.pause()),
        exhaustMap(({ time, autoResume }) => {
          this._currentBagTime$.next(time);
          this._prefetchChunks(time);
          return of({ time, autoResume });
        }),
        tap(({ time, autoResume }) => {
          if (autoResume) {
            this.play();
          } else {
            const prev = addSecToTime(time, -0.033);
            const messages = this._getMessagesInRange(prev, time);
            this._onMessages.next(messages);
          }
        })
      )
      .subscribe();
  }
  get state$() {
    return combineLatest([
      this._currentBagTime$,
      this._bagMetadata$,
      this._options$,
      this._isPlaying$,
    ]).pipe(
      takeUntil(this._destroyInstance$),
      map(([currentTime, bagMetadata, options, isPlaying]) => {
        return {
          currentTime,
          bagMetadata,
          options,
          isPlaying,
        };
      }),
      shareReplay(1)
    );
  }
  get messages$() {
    return this._onMessages
      .asObservable()
      .pipe(takeUntil(this._destroyInstance$));
  }

  loadFile(file: File) {
    this._resetPlayback();
    this._bagInspector.setFile(file);
    this._chunkManager.setFile(file);
  }
  //#region  playBack controls

  play(): void {
    const bagMetadata = this._bagMetadata$.value;
    if (!bagMetadata || this._isPlaying$.value) return;
    if (!this._currentBagTime$.value) {
      this._currentBagTime$.next(bagMetadata.startTime);
    }
    this._wallStartTime = performance.now(); // real time in ms
    let bagTimeAtWallStart = this._currentBagTime$.value; // bag clock reference
    let lastPrefetchTimeSec =
      bagTimeAtWallStart.sec + bagTimeAtWallStart.nsec / 1e9;
    this._isPlaying$.next(true);
    this._playbackInterval$
      .pipe(takeUntil(this._isPlaying$.pipe(filter((res) => !res))))
      .subscribe(() => {
        const elapsedSec = (performance.now() - this._wallStartTime) / 1000; // comparing our initial real time with our current one
        const newBagTime = addSecToTime(
          bagTimeAtWallStart,
          elapsedSec * this._options$.value.playbackSpeed
        );
        const newTimeSec = newBagTime.sec + newBagTime.nsec / 1e9;
        const previousBagTime = addSecToTime(
          bagTimeAtWallStart,
          elapsedSec - 0.033
        );
        if (!isLessThan(newBagTime, bagMetadata.endTime)) {
          if (this._options$.value.loop) {
            const resetTime = bagMetadata.startTime;
            this._currentBagTime$.next(resetTime);
            bagTimeAtWallStart = resetTime;
            this._wallStartTime = performance.now();
            lastPrefetchTimeSec = resetTime.sec + resetTime.nsec / 1e9;
            this._prefetchChunks(resetTime);
            return;
          } else {
            this._isPlaying$.next(false);
            this._currentBagTime$.next(bagMetadata.startTime);
            return;
          }
        }

        this._currentBagTime$.next(newBagTime);
        const messages = this._getMessagesInRange(previousBagTime, newBagTime);
        this._onMessages.next(messages);
        if (
          newTimeSec - lastPrefetchTimeSec >
          this._options$.value.prefetch / 2
        ) {
          this._prefetchChunks(newBagTime);
          lastPrefetchTimeSec = newTimeSec;
        }
      });
  }
  pause() {
    this._isPlaying$.next(false);
  }
  seek(time: ITime) {
    this._cancelPrefetch$.next();
    this._seek$.next({ time, autoResume: this._isPlaying$.value });
  }

  updateOptions(options: IRosbagOptions) {
    this._options$.next({ ...this._options$.value, ...options });
  }

  destroyInstance() {
    this._resetPlayback();
    this._destroyInstance$.next();
    this._destroyInstance$.complete();
    this._bagInspector.destroyInstance();
  }
  //#endregion

  private _prefetchChunks(startTime: ITime): void {
    const prefetchEndTime = addSecToTime(
      startTime,
      this._options$.value.prefetch
    );
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

  private _getMessagesInRange(start: ITime, end: ITime): IRosbagMessage[] {
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
  private _resetPlayback(): void {
    this.pause();
    this._cancelPrefetch$.next();
    this._currentBagTime$.next(null);
  }
}

// todo add filter by topic
