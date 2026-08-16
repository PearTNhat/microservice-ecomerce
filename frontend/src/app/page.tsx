import { Button } from "@/components/ui/button";
import { ProductCard } from "@/features/products/components/product-card";
import { productService } from "@/features/products/services/product-service";
import {
  Airplay,
  ArrowRight,
  Flame,
  FlameKindling,
  Refrigerator,
  Sparkles,
  Tv,
  WashingMachine,
  Wind,
  Zap,
} from "lucide-react";
import Link from "next/link";

const CATEGORY_ICONS: Record<string, any> = {
  "may-lanh-dieu-hoa": Wind,
  "tu-lanh": Refrigerator,
  "may-giat-may-say": WashingMachine,
  "bep-tu-thiet-bi-bep": FlameKindling,
  "smart-tivi": Tv,
};

export default async function HomePage() {
  // Parallel Data Fetching - Tránh waterfall theo chuẩn Vercel
  const [productsRes, categoriesRes] = await Promise.all([
    productService.getProducts({ limit: 8, sort_by: "views" }).catch(() => ({
      data: { products: [] },
    })),
    productService.getCategories().catch(() => ({ data: [] })),
  ]);

  const products = productsRes.data?.products || [];
  const categories = categoriesRes.data || [];

  return (
    <div className="space-y-16 pb-16">
      {/* 1. HERO SECTION */}
      <section className="relative overflow-hidden bg-gradient-to-br from-slate-900 via-blue-950 to-slate-900 text-white py-16 md:py-24">
        <div className="absolute inset-0 bg-[radial-gradient(ellipse_80%_80%_at_50%_-20%,rgba(59,130,246,0.3),rgba(255,255,255,0))]" />
        
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 relative z-10">
          <div className="grid grid-cols-1 lg:grid-cols-12 gap-12 items-center">
            <div className="lg:col-span-7 space-y-6 text-center lg:text-left">
              <div className="inline-flex items-center gap-2 px-3.5 py-1.5 rounded-full bg-blue-500/10 border border-blue-500/20 text-blue-400 text-xs font-bold tracking-wide uppercase">
                <Sparkles className="w-4 h-4" />
                Đại Tiệc Điện Máy Công Nghệ Mới
              </div>

              <h1 className="text-4xl sm:text-5xl lg:text-6xl font-black tracking-tight leading-tight">
                Không Gian Sống Thông Minh{" "}
                <span className="bg-gradient-to-r from-blue-400 via-cyan-400 to-teal-300 bg-clip-text text-transparent">
                  Tiết Kiệm Điện Vượt Trội
                </span>
              </h1>

              <p className="text-base sm:text-lg text-slate-300 max-w-2xl mx-auto lg:mx-0 font-normal">
                Sở hữu ngay các dòng Máy lạnh Inverter lọc khí, Tủ lạnh 4 cánh Twin Cooling và Bếp từ nhập khẩu chính hãng với ưu đãi giảm tới 35%.
              </p>

              <div className="flex flex-col sm:flex-row items-center justify-center lg:justify-start gap-4 pt-2">
                <Link href="/products">
                  <Button size="lg" className="w-full sm:w-auto shadow-lg shadow-blue-500/30 gap-2">
                    Khám Phá Sản Phẩm <ArrowRight className="w-4 h-4" />
                  </Button>
                </Link>
                <Link href="/products?category_id=1">
                  <Button variant="outline" size="lg" className="w-full sm:w-auto text-white border-slate-700 hover:bg-slate-800">
                    Máy Lạnh Daikin & Panasonic
                  </Button>
                </Link>
              </div>

              {/* Highlights */}
              <div className="pt-6 grid grid-cols-3 gap-4 border-t border-slate-800/80 text-center lg:text-left">
                <div>
                  <p className="text-2xl font-black text-white">100%</p>
                  <p className="text-xs text-slate-400 mt-0.5">Chính Hãng</p>
                </div>
                <div>
                  <p className="text-2xl font-black text-white">0₫</p>
                  <p className="text-xs text-slate-400 mt-0.5">Miễn Phí Lắp Đặt</p>
                </div>
                <div>
                  <p className="text-2xl font-black text-white">24 Tháng</p>
                  <p className="text-xs text-slate-400 mt-0.5">Bảo Hành Tận Nhà</p>
                </div>
              </div>
            </div>

            <div className="lg:col-span-5 hidden lg:block relative">
              <div className="relative w-full aspect-square rounded-3xl bg-gradient-to-tr from-blue-600/20 to-cyan-500/20 p-8 border border-slate-700/50 backdrop-blur-xl flex flex-col justify-center items-center shadow-2xl">
                <div className="w-24 h-24 rounded-3xl bg-blue-600/30 flex items-center justify-center text-cyan-300 mb-6 shadow-inner animate-bounce">
                  <Zap className="w-12 h-12" />
                </div>
                <h3 className="text-2xl font-black text-center text-white mb-2">
                  Inverter Tiết Kiệm Đến 65% Điện Năng
                </h3>
                <p className="text-xs text-center text-slate-300">
                  Tất cả sản phẩm điện máy tại ElectroHub đều đạt chuẩn nhãn năng lượng 5 sao cao cấp nhất.
                </p>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* 2. CATEGORIES SECTION */}
      <section className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between mb-8">
          <div>
            <h2 className="text-2xl sm:text-3xl font-black text-slate-900 dark:text-white tracking-tight">
              Danh Mục Nổi Bật
            </h2>
            <p className="text-sm text-slate-500 mt-1">Lựa chọn giải pháp điện máy hoàn hảo cho ngôi nhà bạn</p>
          </div>
          <Link href="/products" className="text-sm font-bold text-blue-600 hover:text-blue-700 inline-flex items-center gap-1">
            Xem tất cả <ArrowRight className="w-4 h-4" />
          </Link>
        </div>

        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-4">
          {categories.map((cat) => {
            const Icon = CATEGORY_ICONS[cat.slug] || Airplay;
            return (
              <Link
                key={cat.id}
                href={`/products?category_id=${cat.id}`}
                className="group p-5 rounded-2xl bg-white dark:bg-slate-900 border border-slate-100 dark:border-slate-800 shadow-sm hover:shadow-xl hover:border-blue-500/40 transition-all duration-300 flex flex-col items-center text-center space-y-3"
              >
                <div className="w-14 h-14 rounded-2xl bg-blue-50 dark:bg-blue-950/40 text-blue-600 dark:text-blue-400 flex items-center justify-center group-hover:scale-110 group-hover:bg-blue-600 group-hover:text-white transition-all duration-300 shadow-sm">
                  <Icon className="w-7 h-7" />
                </div>
                <span className="text-sm font-bold text-slate-800 dark:text-slate-200 group-hover:text-blue-600 transition">
                  {cat.name}
                </span>
              </Link>
            );
          })}
        </div>
      </section>

      {/* 3. HOT PRODUCTS (REDIS CACHE + SINGLEFLIGHT) */}
      <section className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between mb-8">
          <div className="flex items-center gap-2">
            <div className="p-2 rounded-xl bg-rose-500/10 text-rose-600">
              <Flame className="w-6 h-6 fill-current animate-pulse" />
            </div>
            <div>
              <h2 className="text-2xl sm:text-3xl font-black text-slate-900 dark:text-white tracking-tight">
                Sản Phẩm Điện Máy Hot Nhất
              </h2>
              <p className="text-sm text-slate-500 mt-0.5">Top thiết bị được nhiều gia đình tin dùng nhất tuần qua</p>
            </div>
          </div>
          <Link href="/products" className="text-sm font-bold text-blue-600 hover:text-blue-700 hidden sm:inline-flex items-center gap-1">
            Xem tất cả ({products.length}) <ArrowRight className="w-4 h-4" />
          </Link>
        </div>

        {products.length === 0 ? (
          <div className="p-12 text-center bg-white dark:bg-slate-900 rounded-3xl border border-slate-100 dark:border-slate-800">
            <p className="text-slate-500">Đang đồng bộ dữ liệu sản phẩm từ Backend Microservices...</p>
          </div>
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
            {products.map((product) => (
              <ProductCard key={product.id} product={product} />
            ))}
          </div>
        )}
      </section>
    </div>
  );
}
