import { Footer } from "@/components/layouts/footer";
import { Navbar } from "@/components/layouts/navbar";
import { CartDrawer } from "@/features/cart/components/cart-drawer";
import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";

const inter = Inter({
  subsets: ["latin", "vietnamese"],
  variable: "--font-inter",
});

export const metadata: Metadata = {
  title: "ElectroHub - Siêu Thị Điện Máy & Gia Dụng Thông Minh",
  description:
    "Hệ thống mua sắm thiết bị điện máy chính hãng: Máy lạnh Inverter, Tủ lạnh Side by Side, Máy giặt, Bếp từ nhập khẩu, Smart Tivi 4K giá tốt nhất.",
  keywords: ["điện máy", "máy lạnh", "tủ lạnh", "bếp từ", "daikin", "panasonic", "samsung", "bosch"],
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="vi" className={`${inter.variable} h-full antialiased`}>
      <body className="min-h-full flex flex-col font-sans bg-slate-50 dark:bg-slate-950 text-slate-900 dark:text-slate-100">
        <Navbar />
        <main className="flex-1">{children}</main>
        <Footer />
        <CartDrawer />
      </body>
    </html>
  );
}
