// Port of node's lib/internal/async_context_frame.js surface for
// --expose-internals consumers (vendored node tests) and for Bun's own
// built-ins that need to swap the active async-context frame around a
// callback — the JS-side equivalent of the native
// AsyncContextFrame::call / withAsyncContextIfNeeded.
//
// Bun tracks async context natively in the engine (AsyncLocalStorage rides
// JSC's async context), so context propagation is always enabled and the
// "frame" is the raw internal-field value (an even-length [ALS, value, ...]
// array or undefined) — see the comment at the top of node/async_hooks.ts.
// The Bun.ModuleGraph a frame chain is inside of, and the frame that entered its context:
// ModuleGraph.cpp walks the chain (AsyncLocalStorage's frames say nothing about graphs).
const moduleGraphOfFrame = $newCppFunction("ModuleGraph.cpp", "jsFunctionModuleGraphOfFrame", 1);
const moduleGraphFrameOfFrame = $newCppFunction("ModuleGraph.cpp", "jsFunctionModuleGraphFrameOfFrame", 1);

const AsyncContextFrame = {
  enabled: true,
  current() {
    return $getInternalField($asyncContext, 0);
  },
  /**
   * The frame a Bun.ModuleGraph's context was entered with, when the running script is inside
   * one. What something long-lived `run()`s its later work in so that the work is that graph's,
   * without keeping the AsyncLocalStorage stores of whoever happened to create it.
   */
  currentGraphFrame() {
    return moduleGraphFrameOfFrame($getInternalField($asyncContext, 0));
  },
  /** The Bun.ModuleGraph whose context the running script is inside of, if any. */
  currentGraph() {
    return moduleGraphOfFrame($getInternalField($asyncContext, 0));
  },
  /** The Bun.ModuleGraph whose context `frame` (a current() or currentGraphFrame() of earlier) is inside of. */
  graphOf(frame) {
    return moduleGraphOfFrame(frame);
  },
  /** Install `frame` as the active async-context frame; returns the previous one. */
  exchange(frame) {
    const prev = $getInternalField($asyncContext, 0);
    $putInternalField($asyncContext, 0, frame);
    return prev;
  },
  /**
   * Call `fn` with `frame` installed as the active async-context frame,
   * restoring the previous frame afterwards. Fast-paths when `frame` is
   * already active (which includes the "no ALS in use anywhere" case where
   * both are undefined).
   */
  run(frame, fn, thisArg?, ...args) {
    const prev = $getInternalField($asyncContext, 0);
    if (frame === prev) return fn.$apply(thisArg, args);
    $putInternalField($asyncContext, 0, frame);
    try {
      return fn.$apply(thisArg, args);
    } finally {
      $putInternalField($asyncContext, 0, prev);
    }
  },
  /**
   * `run()` for a call that one script left waiting on something another script's event
   * completes: a write parked on a socket until the socket drains or closes. `frame` is only
   * installed when it is inside the same Bun.ModuleGraph as the active frame. The event is its
   * owner's, so what the call goes on to start (the next write, the shutdown, 'close') must not
   * go with a graph that only wrote and was disposed since.
   */
  runInSameGraph(frame, fn, thisArg?, a?, b?, c?) {
    const prev = $getInternalField($asyncContext, 0);
    if (frame === prev || moduleGraphOfFrame(frame) !== moduleGraphOfFrame(prev)) return fn.$call(thisArg, a, b, c);
    $putInternalField($asyncContext, 0, frame);
    try {
      return fn.$call(thisArg, a, b, c);
    } finally {
      $putInternalField($asyncContext, 0, prev);
    }
  },
};

export default AsyncContextFrame;
