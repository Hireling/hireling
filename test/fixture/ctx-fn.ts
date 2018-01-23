import { JobHandle } from '../../src/worker';
import { fnwait } from '../../src/util';

export const ctx = async (jh: JobHandle) => {
  jh.progress(5);

  // test external function available in sandbox
  await fnwait(100);

  return 555;
};
