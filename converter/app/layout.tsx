import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "KTM Converter",
  description: "Intern dashboard voor Shopify-spiegel en jobs",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="nl">
      <body>{children}</body>
    </html>
  );
}
