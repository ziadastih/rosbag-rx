import { RosbagManager } from "../rosbagManager";
import { addSecToTime } from "../utils/timeUtil";

const input = document.getElementById("bag-input");
const startBtn = document.getElementById("start-btn");
const pauseBtn = document.getElementById("pause-btn");
const seekBar = document.getElementById("seek-bar") as HTMLInputElement;
const currentTimeEl = document.getElementById("current-time")!;
const endTimeEl = document.getElementById("end-time")!;

const jsonO = document.getElementById("json-output");
const rosbagManager = new RosbagManager();
let bagStartTime = {
  sec: 0,
  nsec: 0,
};
if (input) {
  input.addEventListener("change", (e) => {
    const event = e.target as HTMLInputElement;
    if (!event.files) return;
    const file = event.files[0];
    rosbagManager.loadFile(file);
  });
}

startBtn.addEventListener("click", () => {
  rosbagManager.play();
});
pauseBtn.addEventListener("click", () => {
  rosbagManager.pause();
});

rosbagManager.messages$.subscribe((res) => {
  console.log(res);
});
rosbagManager.state$.subscribe(({ bagMetadata, currentTime }) => {
  if (!bagMetadata || !currentTime) return;
  bagStartTime = bagMetadata.startTime;
  const start = bagMetadata.startTime;
  const end = bagMetadata.endTime;
  const current = currentTime;
  const startSec = start.sec + start.nsec / 1e9;
  const endSec = end.sec + end.nsec / 1e9;
  const currentSec = current.sec + current.nsec / 1e9;
  seekBar.min = "0";
  seekBar.max = (endSec - startSec).toString();
  seekBar.value = (currentSec - startSec).toString();
  currentTimeEl.textContent = formatTime(current.sec, current.nsec);
  endTimeEl.textContent = formatTime(end.sec, end.nsec);
  bagMetadata.connections.forEach((conn) => {
    rosbagManager.showConnectionMsgs(conn.topicName);
  });
});

seekBar.addEventListener("input", () => {
  const newTime = addSecToTime(bagStartTime, Number(seekBar.value));
  rosbagManager.seek(newTime);
});

function formatTime(sec: number, nsec: number) {
  const date = new Date(sec * 1000 + nsec / 1e6);
  return date.toISOString().substr(11, 12);
}
