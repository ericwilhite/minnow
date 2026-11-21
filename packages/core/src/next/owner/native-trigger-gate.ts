/** SQL row-trigger firing uses the qualified native command path. Unsupported
 * roots retain the consumer's explicit typed refusals; there is no public override. */
export function nativeTriggerFiringEnabled(): boolean {
  return true;
}
