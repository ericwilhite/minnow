"use client";

import { useEffect, useState } from "react";

/**
 * The theme the site is showing. Fumadocs switches themes with a class on `<html>`, and the panel
 * lives in a shadow root that only knows the OS setting unless it is told otherwise — so a reader
 * on a light machine reading these docs in dark mode would get a white panel in a dark page.
 */
export function useSiteTheme(): "light" | "dark" {
  const [theme, setTheme] = useState<"light" | "dark">("light");

  useEffect(() => {
    const root = document.documentElement;
    const read = (): void => {
      setTheme(root.classList.contains("dark") ? "dark" : "light");
    };
    read();
    const observer = new MutationObserver(read);
    observer.observe(root, { attributes: true, attributeFilter: ["class"] });
    return () => {
      observer.disconnect();
    };
  }, []);

  return theme;
}
