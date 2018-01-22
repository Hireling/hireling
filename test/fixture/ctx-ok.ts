import { JobHandle } from '../../src/worker';

export const ctx = async (jh: JobHandle) => {
  jh.progress(5);

  return 555;
};
