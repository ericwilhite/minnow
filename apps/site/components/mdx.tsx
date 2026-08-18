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
