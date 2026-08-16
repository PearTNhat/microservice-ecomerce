import { Headphones, ShieldCheck, Truck, Tv, Wrench } from "lucide-react";
import Link from "next/link";

export function Footer() {
  return (
    <footer className="bg-slate-900 text-slate-400 border-t border-slate-800">
      {/* Guarantees */}
      <div className="border-b border-slate-800/80">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-10">
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
            <div className="flex items-center gap-4 p-4 rounded-2xl bg-slate-800/40 border border-slate-800">
              <div className="w-12 h-12 rounded-xl bg-blue-500/10 text-blue-400 flex items-center justify-center flex-shrink-0">
                <Truck className="w-6 h-6" />
              </div>
              <div>
                <h4 className="text-white text-sm font-bold">Giao Hàng Siêu Tốc</h4>
                <p className="text-xs text-slate-400 mt-0.5">Miễn phí giao toàn quốc 2-4h</p>
              </div>
            </div>

            <div className="flex items-center gap-4 p-4 rounded-2xl bg-slate-800/40 border border-slate-800">
              <div className="w-12 h-12 rounded-xl bg-emerald-500/10 text-emerald-400 flex items-center justify-center flex-shrink-0">
                <ShieldCheck className="w-6 h-6" />
              </div>
              <div>
                <h4 className="text-white text-sm font-bold">Chính Hãng 100%</h4>
                <p className="text-xs text-slate-400 mt-0.5">Bảo hành chính hãng tận nhà</p>
              </div>
            </div>

            <div className="flex items-center gap-4 p-4 rounded-2xl bg-slate-800/40 border border-slate-800">
              <div className="w-12 h-12 rounded-xl bg-amber-500/10 text-amber-400 flex items-center justify-center flex-shrink-0">
                <Wrench className="w-6 h-6" />
              </div>
              <div>
                <h4 className="text-white text-sm font-bold">Lắp Đặt Chuyên Nghiệp</h4>
                <p className="text-xs text-slate-400 mt-0.5">Đội ngũ kỹ thuật viên tận tâm</p>
              </div>
            </div>

            <div className="flex items-center gap-4 p-4 rounded-2xl bg-slate-800/40 border border-slate-800">
              <div className="w-12 h-12 rounded-xl bg-purple-500/10 text-purple-400 flex items-center justify-center flex-shrink-0">
                <Headphones className="w-6 h-6" />
              </div>
              <div>
                <h4 className="text-white text-sm font-bold">Hỗ Trợ 24/7</h4>
                <p className="text-xs text-slate-400 mt-0.5">Hotline: 1800.6868 (Miễn phí)</p>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Main Footer Links */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-8">
          <div className="space-y-4">
            <Link href="/" className="flex items-center gap-2.5">
              <div className="w-9 h-9 rounded-xl bg-blue-600 flex items-center justify-center text-white">
                <Tv className="w-5 h-5" />
              </div>
              <span className="text-xl font-bold text-white tracking-tight">ElectroHub</span>
            </Link>
            <p className="text-xs leading-relaxed text-slate-400">
              Hệ thống bán lẻ thiết bị điện máy, điện lạnh & gia dụng cao cấp chính hãng hàng đầu Việt Nam.
            </p>
          </div>

          <div>
            <h3 className="text-white text-sm font-bold uppercase tracking-wider mb-3">
              Danh Mục Sản Phẩm
            </h3>
            <ul className="space-y-2 text-xs">
              <li><Link href="/products?category_id=1" className="hover:text-white transition">Máy Lạnh - Điều Hòa</Link></li>
              <li><Link href="/products?category_id=2" className="hover:text-white transition">Tủ Lạnh Multidoor</Link></li>
              <li><Link href="/products?category_id=3" className="hover:text-white transition">Máy Giặt Inverter</Link></li>
              <li><Link href="/products?category_id=4" className="hover:text-white transition">Bếp Từ & Thiết Bị Bếp</Link></li>
              <li><Link href="/products?category_id=5" className="hover:text-white transition">Smart Tivi 4K QLED</Link></li>
            </ul>
          </div>

          <div>
            <h3 className="text-white text-sm font-bold uppercase tracking-wider mb-3">
              Chính Sách & Hỗ Trợ
            </h3>
            <ul className="space-y-2 text-xs">
              <li><Link href="#" className="hover:text-white transition">Chính sách bảo hành tận nhà</Link></li>
              <li><Link href="#" className="hover:text-white transition">Chính sách đổi trả trong 30 ngày</Link></li>
              <li><Link href="#" className="hover:text-white transition">Hướng dẫn mua hàng trả góp 0%</Link></li>
              <li><Link href="#" className="hover:text-white transition">Tra cứu trạng thái đơn hàng</Link></li>
            </ul>
          </div>

          <div>
            <h3 className="text-white text-sm font-bold uppercase tracking-wider mb-3">
              Tổng Đài Hỗ Trợ
            </h3>
            <div className="space-y-2 text-xs">
              <p>Mua hàng: <span className="text-white font-semibold">1800 6868</span></p>
              <p>Bảo hành: <span className="text-white font-semibold">1800 6869</span></p>
              <p>Email: <span className="text-white font-semibold">support@electrohub.vn</span></p>
            </div>
          </div>
        </div>

        <div className="mt-12 pt-8 border-t border-slate-800 text-center text-xs text-slate-500">
          © {new Date().getFullYear()} ElectroHub E-Commerce Microservices. All rights reserved.
        </div>
      </div>
    </footer>
  );
}
