import {
  BehaviorSubject,
  combineLatest,
  exhaustMap,
  filter,
  interval,
  map,
  of,
  shareReplay,
  Subject,
  takeUntil,
  tap,
} from "rxjs";
import { IBagMetadata, IBagReader } from "./models/rosbag-manager.models";
import { IRosbagMessage, IRosbagOptions, ITime } from "./models/general.models";
import { addSecToTime, isLessThan } from "./utils/timeUtil";
import { Ros1BagReader } from "./ros1/ros1-bag-reader";

export class RosbagManager {
  private _bagReader: IBagReader;
  private _bagMetadata$ = new BehaviorSubject<IBagMetadata | null>(null);
  private _options$ = new BehaviorSubject<IRosbagOptions>({
    prefetch: 10,
    playbackSpeed: 1,
    loop: true,
  });
  private _filteredConnections = new Set();
  private _isPlaying$ = new BehaviorSubject<boolean>(false);
  private _currentBagTime$ = new BehaviorSubject<ITime | null>(null);
  private _wallStartTime = 0;
  private _playbackInterval$ = interval(33);
  private _onMessages = new Subject<IRosbagMessage[]>();
  private _destroyInstance$ = new Subject<void>();
  private _seek$ = new Subject<{ time: ITime; autoResume: boolean }>();
  private _cancelPrefetch$ = new Subject<void>();

  constructor() {
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
    return this._onMessages.asObservable().pipe(
      takeUntil(this._destroyInstance$),
      map((msgs) => {
        return msgs.filter((msg) => this._filteredConnections.has(msg.topic));
      })
    );
  }
  get error$() {
    return this._bagReader.error$.pipe(takeUntil(this._destroyInstance$));
  }

  loadFile(file: File) {
    this._resetPlayback();
    this._filteredConnections.clear();
    const format = file.name.split(".")?.pop();
    this._bagReader?.destroyReader();
    switch (format) {
      case "bag": // ros1
        this._bagReader = null;
        this._bagReader = new Ros1BagReader();
        this._bagReader.loadFile(file);
        break;
      case "mcap": // ros2 mcap
        break;
    }
    // todo verify this memory leak
    this._bagReader.metadata$.subscribe((res) => {
      this._bagMetadata$.next(res);
      this._currentBagTime$.next(res.startTime);
      this._prefetchChunks(res.startTime);
    });
  }

  //#region playback controls

  play(): void {
    const bagMetadata = this._bagMetadata$.value;
    if (!bagMetadata || this._isPlaying$.value) return;
    if (!this._currentBagTime$.value) {
      this._currentBagTime$.next(bagMetadata.startTime);
    }
    let lastElapsedSec = 0;
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
          lastElapsedSec * this._options$.value.playbackSpeed
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
        lastElapsedSec = elapsedSec;
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

  private _resetPlayback(): void {
    this.pause();
    this._cancelPrefetch$.next();
    this._currentBagTime$.next(null);
  }
  //#endregion

  private _prefetchChunks(startTime: ITime): void {
    this._bagReader.prefetchChunks(startTime, this._options$.value.prefetch);
  }

  private _getMessagesInRange(start: ITime, end: ITime): IRosbagMessage[] {
    return this._bagReader.getMessagesInRange(start, end);
  }

  showConnectionMsgs(connection: string) {
    this._filteredConnections.add(connection);
  }
  hideConnectionMsgs(connection: string) {
    this._filteredConnections.delete(connection);
  }
}
