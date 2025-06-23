# rosbag-rx

**A high-performance, frontend-focused ROS bag playback library for JavaScript and TypeScript.**

---

## 🚀 Install

```bash
npm install rosbag-rx
```

## 🎯 Purpose
rosbag-rx brings the simplicity of rosbag play to the browser and modern frontend environments.

It removes the need to manually handle .bag file parsing, message decoding, or timing synchronization — allowing you to focus on building robotics tools instead of reinventing playback logic.

With just a few lines of code, you can load a .bag file, extract metadata, and stream messages efficiently in real time — with zero playback delay.

## 📦 Features

- 🎯 Simulates `rosbag play` in browser/JavaScript
- 🕒 Real-time playback with accurate timing
- 🔁 `play()`, `pause()`, `seek()` and looping support
- ⚡ Zero-delay streaming via RxJS observables
- 🧠 Automatic memory and subscription management
- 🔌 Easily toggle topic streams
- 🧹 Built-in cleanup mechanism


## 🧪 Advanced Usage (RosbagManager)

The `RosbagManager` class is the main interface for high-level bag playback. It handles state management, playback, message streaming, and cleanup — all through a clean RxJS-based API.

You can reuse the same `RosbagManager` instance for multiple files. It will automatically reset internal state when a new file is loaded.

---

### 🧱 Initialize

```ts
import { RosbagManager } from 'rosbag-rx';

const rosbagManager = new RosbagManager();
```
---

### 📡 Subscribe to Bag State

You should subscribe to `state$` before calling `loadFile()`.

This stream includes bag metadata, playback state, and current playback time.
No need to manually unsubscribe — everything is cleaned up automatically when `destroyInstance()` is called.

```ts
rosbagManager.state$.subscribe({
  next: (state) => {
    console.log(state);
    // {
    //   currentTime: { sec, nsec },
    //   bagMetadata: {
    //     startTime: { sec, nsec },
    //     endTime: { sec, nsec },
    //     connections: Map(...),
    //     chunksInfo: [...]
    //   },
    //   options: {
    //     prefetch: 10,
    //     playbackSpeed: 1,
    //     loop: true
    //   },
    //   isPlaying: false
    // }
  }
});
```

---

### ⚠️ Handle Errors

You can subscribe to `error$` to receive well-explained errors such as:

* Corrupted bag file
* Parsing issues
* Unsupported or malformed message structures

```ts
rosbagManager.error$.subscribe({
  next: (error) => {
    console.error('Bag error:', error);
  }
});
```

---

### 📨 Subscribe to Streamed Messages

You will receive an array of messages for the **currently active time slice**, filtered by the topics you’ve toggled using `showConnectionMsgs`.

```ts
rosbagManager.messages$.subscribe({
  next: (messages) => {
    console.log('Received messages:', messages);
  }
});
```

---

### 📂 Load and Control Playback

```ts
await rosbagManager.loadFile(file); // File, Blob, or ArrayBuffer

rosbagManager.play();
rosbagManager.pause();
rosbagManager.seek({ sec: 0, nsec: 0 }); // Seek to start
```

---

### ⚙️ Update Playback Options

```ts
rosbagManager.updateOptions({
  playbackSpeed: 5, // Default is 1
  prefetch: 1,      // Default is 10 seconds (recommended)
  loop: false       // Default is true
});
```

---

### 🎛️ Toggle Topic Streams

By default, **all topics are disabled**. To receive messages, you must explicitly enable them.

#### Show a topic:

```ts
rosbagManager.showConnectionMsgs('/odom');
```

#### Hide a topic:

```ts
rosbagManager.hideConnectionMsgs('/odom');
```

---

### 🧹 Clean Up

To prevent memory leaks and stop all internal observables, call:

```ts
rosbagManager.destroyInstance();
```

This will:

* Unsubscribe from all internal streams
* Reset internal state
* Release all resources
