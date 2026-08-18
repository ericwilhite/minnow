import type { ComponentProps } from "react";
import defaultMdxComponents from "fumadocs-ui/mdx";
import { Callout } from "fumadocs-ui/components/callout";
import { Step, Steps } from "fumadocs-ui/components/steps";
import { Tab, Tabs } from "fumadocs-ui/components/tabs";
import { TypeTable } from "fumadocs-ui/components/type-table";
import type { MDXComponents } from "mdx/types";
import { DevtoolsDemo } from "@/components/playground/devtools-demo";
import { Playground } from "@/components/playground/playground";
import { SqlFeatureMatrix } from "@/components/sql-feature-matrix";

/**
 * The anchor a link in MDX renders as.
 *
 * A link to something the site *serves* rather than routes — `/llms.txt`, `/agent-rules.md`, a
 * page's markdown twin — is a file. Handed to the router it is resolved as an app route instead:
 * the extension is dropped and the reader lands on the 404 for a URL they never clicked. Those
 * get a plain anchor, and so a real request; every other link keeps client-side navigation.
 */
export function MdxLink({ href, ...props }: ComponentProps<"a">) {
  const isFile = href !== undefined && href.startsWith("/") && /\.[a-z0-9]+(?:[?#]|$)/i.test(href);
  if (isFile) return <a href={href} {...props} />;
  const Link = defaultMdxComponents.a;
  return <Link href={href} {...props} />;
}

/**
 * What MDX pages can reach for without importing anything. The docs stay close to plain
 * markdown; these are the few pieces that would otherwise be repeated markup.
 */
export function getMDXComponents(components?: MDXComponents): MDXComponents {
  return {
    ...defaultMdxComponents,
    Callout,
    Step,
    Steps,
    Tab,
    Tabs,
    TypeTable,
    DevtoolsDemo,
    Playground,
    SqlFeatureMatrix,
    ...components,
  };
}

export const useMDXComponents = getMDXComponents;
