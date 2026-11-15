/** Complete mandatory FTS publication/checkpoint/retirement step, alongside native row work.
 * Two held journal pages, bounded FTS tree walk, output journal codec, descriptors and IO.
 * Token preparation is optional additional whole-step admission; failure invalidates instead.
 */
export const FTS_PUBLICATION_MINIMUM_BYTES = 4 * 1024 * 1024;
