import { Badge } from "@/components/ui/badge";
import { AddToCartButton } from "@/features/products/components/add-to-cart-button";
import { SpecsTable } from "@/features/products/components/specs-table";
import { productService } from "@/features/products/services/product-service";
import { calculateDiscount, formatPrice } from "@/lib/utils";
import {
  CheckCircle2,
  ChevronRight,
  Eye,
  RotateCcw,
  ShieldCheck,
  Star,
  Truck,
  Wrench,
  Zap,
} from "lucide-react";
import Image from "next/image";
import Link from "next/link";
import { notFound } from "next/navigation";

interface ProductDetailPageProps {
  params: Promise<{ id: string }>;
}

export async function generateMetadata({ params }: ProductDetailPageProps) {
  const { id } = await params;
  try {
    const res = await productService.getProductDetail(Number(id));
    const product = res.data;
    return {
      title: `${product.name} - ElectroHub`,
      description: product.description || `Xem chi tiết và thông số kỹ thuật của ${product.name}`,
    };
  } catch {
    return {
      title: "Chi tiết sản phẩm - ElectroHub",
    };
  }
}

export default async function ProductDetailPage({ params }: ProductDetailPageProps) {
  const { id } = await params;
  const productId = Number(id);

  if (isNaN(productId) || productId <= 0) {
    notFound();
  }

  let product;
  try {
    const res = await productService.getProductDetail(productId);
    product = res.data;
  } catch (error) {
    notFound();
  }

  if (!product) {
    notFound();
  }

  const discountPercent = calculateDiscount(product.price, product.discount_price);
  const currentPrice = product.discount_price || product.price;

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 space-y-10">
      {/* Breadcrumb */}
      <nav className="flex items-center gap-2 text-xs text-slate-500 font-medium overflow-x-auto">
        <Link href="/" className="hover:text-blue-600">
          Trang chủ
        </Link>
        <ChevronRight className="w-3.5 h-3.5 flex-shrink-0" />
        <Link href="/products" className="hover:text-blue-600">
          Sản phẩm
        </Link>
        {product.category && (
          <>
            <ChevronRight className="w-3.5 h-3.5 flex-shrink-0" />
            <Link
              href={`/products?category_id=${product.category.id}`}
              className="hover:text-blue-600"
            >
              {product.category.name}
            </Link>
          </>
        )}
        <ChevronRight className="w-3.5 h-3.5 flex-shrink-0" />
        <span className="text-slate-900 dark:text-white font-bold truncate">
          {product.name}
        </span>
      </nav>

      {/* Main Product Info Section */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-10">
        {/* Left: Image Gallery */}
        <div className="lg:col-span-6 space-y-4">
          <div className="relative aspect-square w-full rounded-3xl overflow-hidden bg-white dark:bg-slate-900 border border-slate-100 dark:border-slate-800 shadow-md">
            {product.thumbnail ? (
              <Image
                src={product.thumbnail}
                alt={product.name}
                fill
                priority
                className="object-cover"
                sizes="(max-width: 1024px) 100vw, 50vw"
              />
            ) : (
              <div className="w-full h-full flex items-center justify-center text-slate-300">
                Ảnh sản phẩm
              </div>
            )}

            {/* Badges */}
            <div className="absolute top-4 left-4 flex flex-col gap-1.5 z-10">
              {discountPercent > 0 && (
                <Badge variant="danger" className="text-sm px-3 py-1 font-black shadow-md">
                  Giảm {discountPercent}%
                </Badge>
              )}
              {product.brand && (
                <Badge variant="default" className="text-xs px-3 py-1 bg-white/95 dark:bg-slate-900/95 backdrop-blur-md shadow-md">
                  Hãng: {product.brand.name}
                </Badge>
              )}
            </div>

            {/* Live Views */}
            <div className="absolute bottom-4 right-4 z-10 flex items-center gap-1.5 bg-black/60 backdrop-blur-md text-white px-3 py-1 rounded-full text-xs font-semibold">
              <Eye className="w-4 h-4 text-cyan-400" />
              <span>{product.views} lượt xem</span>
            </div>
          </div>
        </div>

        {/* Right: Details & Purchase */}
        <div className="lg:col-span-6 space-y-6">
          <div className="space-y-2">
            <div className="flex items-center gap-2 text-amber-400 text-sm font-semibold">
              <Star className="w-4 h-4 fill-current" />
              <span>{product.rating || 5.0}</span>
              <span className="text-slate-400 font-normal">({product.views + 25} đánh giá từ khách hàng)</span>
            </div>

            <h1 className="text-2xl sm:text-3xl font-black text-slate-900 dark:text-white leading-snug">
              {product.name}
            </h1>

            {product.description && (
              <p className="text-sm text-slate-600 dark:text-slate-300 leading-relaxed pt-2">
                {product.description}
              </p>
            )}
          </div>

          {/* Pricing Box */}
          <div className="p-5 rounded-2xl bg-blue-50/50 dark:bg-slate-900 border border-blue-100 dark:border-slate-800 space-y-2">
            <div className="flex items-baseline gap-3">
              <span className="text-3xl sm:text-4xl font-black text-blue-600 dark:text-blue-400">
                {formatPrice(currentPrice)}
              </span>
              {product.discount_price && product.discount_price < product.price && (
                <span className="text-base text-slate-400 line-through">
                  {formatPrice(product.price)}
                </span>
              )}
            </div>
            <div className="flex items-center gap-2 text-xs font-medium text-emerald-600 dark:text-emerald-400">
              <CheckCircle2 className="w-4 h-4" />
              <span>Giá đã bao gồm VAT và gói bảo hành chính hãng tận nơi</span>
            </div>
          </div>

          {/* Add to Cart Client Action */}
          <AddToCartButton product={product} />

          {/* Guarantees Box */}
          <div className="grid grid-cols-2 gap-3 pt-4 border-t border-slate-100 dark:border-slate-800 text-xs">
            <div className="flex items-center gap-2.5 p-3 rounded-xl bg-slate-50 dark:bg-slate-800/40">
              <Truck className="w-4 h-4 text-blue-600 flex-shrink-0" />
              <span className="text-slate-700 dark:text-slate-300">Giao hàng & lắp đặt trong ngày</span>
            </div>
            <div className="flex items-center gap-2.5 p-3 rounded-xl bg-slate-50 dark:bg-slate-800/40">
              <ShieldCheck className="w-4 h-4 text-emerald-600 flex-shrink-0" />
              <span className="text-slate-700 dark:text-slate-300">Cam kết 100% chính hãng</span>
            </div>
            <div className="flex items-center gap-2.5 p-3 rounded-xl bg-slate-50 dark:bg-slate-800/40">
              <RotateCcw className="w-4 h-4 text-amber-600 flex-shrink-0" />
              <span className="text-slate-700 dark:text-slate-300">Đổi mới 1 - 1 trong 30 ngày</span>
            </div>
            <div className="flex items-center gap-2.5 p-3 rounded-xl bg-slate-50 dark:bg-slate-800/40">
              <Wrench className="w-4 h-4 text-purple-600 flex-shrink-0" />
              <span className="text-slate-700 dark:text-slate-300">Bảo dưỡng định kỳ miễn phí</span>
            </div>
          </div>
        </div>
      </div>

      {/* Specifications Section */}
      <div className="pt-10 border-t border-slate-200 dark:border-slate-800 space-y-6">
        <h2 className="text-xl sm:text-2xl font-black text-slate-900 dark:text-white">
          Thông Số Kỹ Thuật Đầy Đủ
        </h2>
        <div className="max-w-4xl">
          <SpecsTable specifications={product.specifications} />
        </div>
      </div>
    </div>
  );
}
