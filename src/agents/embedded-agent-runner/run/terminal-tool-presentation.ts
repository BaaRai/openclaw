type TerminalPresentationObservation = {
  terminalPresentation?: string;
  toolCallOrdinal?: number;
};

export function createTerminalToolPresentationTracker() {
  let latestOrdinal = -1;
  let nextOrdinal = 0;
  let value: string | undefined;
  return {
    allocateOrdinal: () => nextOrdinal++,
    observe: (observation: TerminalPresentationObservation): void => {
      const ordinal = observation.toolCallOrdinal ?? latestOrdinal + 1;
      if (ordinal >= latestOrdinal) {
        latestOrdinal = ordinal;
        value = observation.terminalPresentation;
      }
    },
    read: () => value,
  };
}
