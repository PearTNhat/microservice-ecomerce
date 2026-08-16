import { Badge } from "@/components/ui/badge";
import { ProductCard } from "@/features/products/components/product-card";
import { productService } from "@/features/products/services/product-service";
import { ProductFilterParams } from "@/features/products/types";
import { Filter, Search, SlidersHorizontal } from "lucide-react";
import Link from "next/link";

export const metadata = {
  title: "Danh Sách Sản Phẩm Điện Máy & Thiết Bị Gia Dụng - ElectroHub",
  description: "Khám phá danh mục đầy đủ các dòng máy lạnh, tủ lạnh, bếp từ, tivi cao cấp chính hãng.",
};

interface ProductsPageProps {
  searchParams: Promise<{ [key: string]: string | string[] | undefined }>;
}

export default async function ProductsPage({ searchParams }: ProductsPageProps) {
  const resolvedParams = await searchParams;

  const filterParams: ProductFilterParams = {
    category_id: resolvedParams.category_id ? Number(resolvedParams.category_id) : undefined,
    brand_id: resolvedParams.brand_id ? Number(resolvedParams.brand_id) : undefined,
    min_price: resolvedParams.min_price ? Number(resolvedParams.min_price) : undefined,
    max_price: resolvedParams.max_price ? Number(resolvedParams.max_price) : undefined,
    keyword: typeof resolvedParams.keyword === "string" ? resolvedParams.keyword : undefined,
    sort_by: (resolvedParams.sort_by as any) || "newest",
    page: resolvedParams.page ? Number(resolvedParams.page) : 1,
    limit: 12,
  };

  // Parallel Fetching
  const [productsRes, categoriesRes, brandsRes] = await Promise.all([
    productService.getProducts(filterParams).catch(() => ({
      data: { products: [], total: 0, page: 1, limit: 12, total_pages: 1 },
    })),
    productService.getCategories().catch(() => ({ data: [] })),
    productService.getBrands().catch(() => ({ data: [] })),
  ]);

  const { products, total, total_pages, page } = productsRes.data || {
    products: [],
    total: 0,
    total_pages: 1,
    page: 1,
  };
  const categories = categoriesRes.data || [];
  const brands = brandsRes.data || [];

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 space-y-8">
      {/* Header & Active Filters */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 pb-6 border-b border-slate-200 dark:border-slate-800">
        <div>
          <h1 className="text-2xl sm:text-3xl font-black text-slate-900 dark:text-white tracking-tight">
            {filterParams.keyword
              ? `Kết quả tìm kiếm: "${filterParams.keyword}"`
              : "Danh Sách Sản Phẩm Điện Máy"}
          </h1>
          <p className="text-sm text-slate-500 mt-1">
            Hiển thị <strong>{products.length}</strong> trên tổng số <strong>{total}</strong> sản phẩm
          </p>
        </div>

        {/* Sorting Links */}
        <div className="flex items-center gap-2 overflow-x-auto text-xs font-semibold">
          <span className="text-slate-400 flex items-center gap-1 flex-shrink-0">
            <SlidersHorizontal className="w-3.5 h-3.5" /> Sắp xếp:
          </span>
          <Link
            href={{ query: { ...resolvedParams, sort_by: "newest" } }}
            className={`px-3 py-1.5 rounded-lg border transition ${
              filterParams.sort_by === "newest"
                ? "bg-blue-600 text-white border-blue-600"
                : "bg-white dark:bg-slate-900 text-slate-600 dark:text-slate-300 border-slate-200 dark:border-slate-800"
            }`}
          >
            Mới nhất
          </Link>
          <Link
            href={{ query: { ...resolvedParams, sort_by: "views" } }}
            className={`px-3 py-1.5 rounded-lg border transition ${
              filterParams.sort_by === "views"
                ? "bg-blue-600 text-white border-blue-600"
                : "bg-white dark:bg-slate-900 text-slate-600 dark:text-slate-300 border-slate-200 dark:border-slate-800"
            }`}
          >
            Xem nhiều
          </Link>
          <Link
            href={{ query: { ...resolvedParams, sort_by: "price_asc" } }}
            className={`px-3 py-1.5 rounded-lg border transition ${
              filterParams.sort_by === "price_asc"
                ? "bg-blue-600 text-white border-blue-600"
                : "bg-white dark:bg-slate-900 text-slate-600 dark:text-slate-300 border-slate-200 dark:border-slate-800"
            }`}
          >
            Giá tăng dần
          </Link>
          <Link
            href={{ query: { ...resolvedParams, sort_by: "price_desc" } }}
            className={`px-3 py-1.5 rounded-lg border transition ${
              filterParams.sort_by === "price_desc"
                ? "bg-blue-600 text-white border-blue-600"
                : "bg-white dark:bg-slate-900 text-slate-600 dark:text-slate-300 border-slate-200 dark:border-slate-800"
            }`}
          >
            Giá giảm dần
          </Link>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-12 gap-8">
        {/* Sidebar Filters */}
        <aside className="lg:col-span-3 space-y-6">
          {/* Categories Filter */}
          <div className="p-5 bg-white dark:bg-slate-900 rounded-2xl border border-slate-100 dark:border-slate-800 shadow-sm space-y-3">
            <h3 className="text-sm font-bold text-slate-900 dark:text-white uppercase tracking-wider">
              Danh Mục
            </h3>
            <ul className="space-y-1.5 text-sm">
              <li>
                <Link
                  href="/products"
                  className={`block px-3 py-2 rounded-xl transition ${
                    !filterParams.category_id
                      ? "bg-blue-50 dark:bg-blue-950/50 text-blue-600 font-bold"
                      : "text-slate-600 dark:text-slate-400 hover:bg-slate-50 dark:hover:bg-slate-800"
                  }`}
                >
                  Tất cả danh mục
                </Link>
              </li>
              {categories.map((c) => (
                <li key={c.id}>
                  <Link
                    href={{ query: { ...resolvedParams, category_id: c.id } }}
                    className={`block px-3 py-2 rounded-xl transition ${
                      filterParams.category_id === c.id
                        ? "bg-blue-50 dark:bg-blue-950/50 text-blue-600 font-bold"
                        : "text-slate-600 dark:text-slate-400 hover:bg-slate-50 dark:hover:bg-slate-800"
                    }`}
                  >
                    {c.name}
                  </Link>
                </li>
              ))}
            </ul>
          </div>

          {/* Brands Filter */}
          <div className="p-5 bg-white dark:bg-slate-900 rounded-2xl border border-slate-100 dark:border-slate-800 shadow-sm space-y-3">
            <h3 className="text-sm font-bold text-slate-900 dark:text-white uppercase tracking-wider">
              Thương Hiệu
            </h3>
            <div className="flex flex-wrap gap-2">
              {brands.map((b) => (
                <Link
                  key={b.id}
                  href={{ query: { ...resolvedParams, brand_id: b.id } }}
                  className={`px-3 py-1.5 rounded-xl text-xs font-semibold border transition ${
                    filterParams.brand_id === b.id
                      ? "bg-blue-600 text-white border-blue-600 shadow-sm"
                      : "bg-slate-50 dark:bg-slate-800 text-slate-700 dark:text-slate-300 border-slate-200 dark:border-slate-700 hover:border-blue-500"
                  }`}
                >
                  {b.name}
                </Link>
              ))}
            </div>
          </div>

          {/* Price Range Filter */}
          <div className="p-5 bg-white dark:bg-slate-900 rounded-2xl border border-slate-100 dark:border-slate-800 shadow-sm space-y-3">
            <h3 className="text-sm font-bold text-slate-900 dark:text-white uppercase tracking-wider">
              Mức Giá
            </h3>
            <ul className="space-y-1.5 text-xs">
              <li>
                <Link
                  href={{ query: { ...resolvedParams, min_price: undefined, max_price: 10000000 } }}
                  className="block px-3 py-1.5 rounded-lg text-slate-600 dark:text-slate-400 hover:text-blue-600 hover:bg-slate-50 dark:hover:bg-slate-800"
                >
                  Dưới 10 Triệu
                </Link>
              </li>
              <li>
                <Link
                  href={{ query: { ...resolvedParams, min_price: 10000000, max_price: 15000000 } }}
                  className="block px-3 py-1.5 rounded-lg text-slate-600 dark:text-slate-400 hover:text-blue-600 hover:bg-slate-50 dark:hover:bg-slate-800"
                >
                  Từ 10 - 15 Triệu
                </Link>
              </li>
              <li>
                <Link
                  href={{ query: { ...resolvedParams, min_price: 15000000, max_price: 25000000 } }}
                  className="block px-3 py-1.5 rounded-lg text-slate-600 dark:text-slate-400 hover:text-blue-600 hover:bg-slate-50 dark:hover:bg-slate-800"
                >
                  Từ 15 - 25 Triệu
                </Link>
              </li>
              <li>
                <Link
                  href={{ query: { ...resolvedParams, min_price: 25000000, max_price: undefined } }}
                  className="block px-3 py-1.5 rounded-lg text-slate-600 dark:text-slate-400 hover:text-blue-600 hover:bg-slate-50 dark:hover:bg-slate-800"
                >
                  Trên 25 Triệu
                </Link>
              </li>
            </ul>
          </div>
        </aside>

        {/* Product Grid */}
        <main className="lg:col-span-9 space-y-8">
          {products.length === 0 ? (
            <div className="p-16 text-center bg-white dark:bg-slate-900 rounded-3xl border border-slate-100 dark:border-slate-800 space-y-4">
              <Search className="w-12 h-12 text-slate-300 mx-auto" />
              <h3 className="text-lg font-bold text-slate-900 dark:text-white">
                Không tìm thấy sản phẩm phù hợp
              </h3>
              <p className="text-sm text-slate-500 max-w-md mx-auto">
                Hãy thử kiểm tra lại từ khóa hoặc xóa các bộ lọc danh mục và khoảng giá để xem nhiều sản phẩm hơn.
              </p>
              <Link
                href="/products"
                className="inline-block px-5 py-2.5 bg-blue-600 text-white rounded-xl text-sm font-semibold shadow-sm"
              >
                Xem tất cả sản phẩm
              </Link>
            </div>
          ) : (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-6">
              {products.map((p) => (
                <ProductCard key={p.id} product={p} />
              ))}
            </div>
          )}

          {/* Pagination */}
          {total_pages > 1 && (
            <div className="flex items-center justify-center gap-2 pt-6">
              {Array.from({ length: total_pages }, (_, i) => i + 1).map((p) => (
                <Link
                  key={p}
                  href={{ query: { ...resolvedParams, page: p } }}
                  className={`w-10 h-10 rounded-xl flex items-center justify-center text-sm font-bold transition ${
                    page === p
                      ? "bg-blue-600 text-white shadow-md shadow-blue-500/25"
                      : "bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 text-slate-700 dark:text-slate-300 hover:bg-slate-50"
                  }`}
                >
                  {p}
                </Link>
              ))}
            </div>
          )}
        </main>
      </div>
    </div>
  );
}
