import { z, defineProgram, defineSurface } from '@hanger-source/dsh-reactive';

export const program = defineProgram({ name: 'browser_request_contracts',
  input: z.object({ at: z.iso.datetime().transform(value => new Date(value)) }).strict(),
  surface: defineSurface({ views: {}, actions: {} }),
});
