import { defineAction, defineProgram, defineResource, defineSurface, resourceView, z } from '@hanger-source/dsh-reactive';

export const SiteReading = z.object({ siteId: z.string().min(1), revision: z.number().int().positive(),
  condition: z.enum(['normal', 'anomaly']), detail: z.string() }).strict();
export const InspectionTarget = z.object({ workspaceId: z.string().min(1), siteId: z.string().min(1) }).strict();
export type InspectionTarget = z.output<typeof InspectionTarget>;
export const InspectionReport = z.object({ siteId: z.string().min(1), revision: z.number().int().positive(),
  inspectionId: z.string().min(1), needsReview: z.boolean(), summary: z.string() }).strict();

export const SiteFacts = defineResource({ name: 'inspection.site', params: z.object({ siteId: z.string().min(1) }).strict(), value: SiteReading });
export const Inspect = defineAction({ name: 'inspect_site', description: '核验指定站点的当前修订并记录一次巡检',
  scope: InspectionTarget, input: z.object({ expectedRevision: z.number().int().positive() }).strict(), output: InspectionReport });
export const DiscussReport = defineAction({ name: 'discuss_inspection', description: '围绕原巡检询问人的意见并记录，正式业务权限另由 owner 核验',
  scope: InspectionTarget, input: z.object({ inspectionId: z.string().min(1), question: z.string().min(1) }).strict(),
  output: z.object({ inspectionId: z.string().min(1), recorded: z.literal(true) }).strict() });
export const InspectionSurface = defineSurface({ views: { facts: resourceView(SiteReading) }, actions: { inspect: Inspect, discuss: DiscussReport } });
export const InspectionProgram = defineProgram({ input: InspectionTarget, surface: InspectionSurface });
