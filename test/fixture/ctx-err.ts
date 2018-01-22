import { JobHandle } from '../../src/worker';

export const ctx = async (jh: JobHandle) => {
  jh.progress(5);

  throw new Error('an error');
};
