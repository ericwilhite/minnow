import { notFound } from "next/navigation";
import type { Metadata } from "next";
import { DocsBody, DocsDescription, DocsPage, DocsTitle } from "fumadocs-ui/layouts/notebook/page";
import { createRelativeLink } from "fumadocs-ui/mdx";
import { getMDXComponents, MdxLink } from "@/components/mdx";
import { source } from "@/lib/source";
import { basePath } from "@/lib/versions";

export default async function Page(props: { params: Promise<{ slug?: string[] }> }) {
  const { slug } = await props.params;
  const page = source.getPage(slug);
  if (!page) notFound();

  const MDX = page.data.body;
  return (
    <DocsPage toc={page.data.toc} full={page.data.full}>
      <DocsTitle>{page.data.title}</DocsTitle>
      <DocsDescription>{page.data.description}</DocsDescription>
      <DocsBody>
        <MDX components={getMDXComponents({ a: createRelativeLink(source, page, MdxLink) })} />
      </DocsBody>
    </DocsPage>
  );
}

export function generateStaticParams() {
  return source.generateParams();
}

export async function generateMetadata(props: {
  params: Promise<{ slug?: string[] }>;
}): Promise<Metadata> {
  const { slug } = await props.params;
  const page = source.getPage(slug);
  if (!page) notFound();

  // Every page is published twice: as this page, and as the markdown it was written in, one path
  // segment away. scripts/generate-llms.mjs writes the second one, and this is where a reader
  // that prefers markdown — an agent fetching the page, usually — is told it exists.
  const pathname =
    slug === undefined || slug.length === 0 ? "/docs/index" : `/docs/${slug.join("/")}`;
  return {
    title: page.data.title,
    description: page.data.description,
    alternates: {
      canonical: `${basePath}${pathname === "/docs/index" ? "/docs" : pathname}/`,
      types: { "text/markdown": `${basePath}${pathname}.md` },
    },
  };
}
