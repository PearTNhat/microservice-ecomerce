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

import { flashSaleService } from "@/features/flash-sale/services/flash-sale-service";

export default async function ProductDetailPage({ params }: ProductDetailPageProps) {
  const { id } = await params;
  const productId = Number(id);

  if (isNaN(productId) || productId <= 0) {
    notFound();
  }

  let product;
  let offer = null;
  try {
    const [productRes, offerRes] = await Promise.all([
      productService.getProductDetail(productId),
      flashSaleService.getProductOffer(productId).catch(() => null),
    ]);
    product = productRes.data;
    if (offerRes && offerRes.data && offerRes.data.has_flash_sale) {
      offer = offerRes.data;
    }
  } catch (error) {
    notFound();
  }

  if (!product) {
    notFound();
  }

  const hasFlashSale = Boolean(offer && offer.has_flash_sale);
  const currentPrice = offer && offer.has_flash_sale
    ? offer.sale_price
    : product.discount_price || product.price;

  const originalPrice = product.price;
  const discountPercent = offer && offer.has_flash_sale && offer.discount_percentage
    ? offer.discount_percentage
    : calculateDiscount(product.price, product.discount_price);

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
              {hasFlashSale ? (
                <div className="inline-flex items-center gap-1.5 bg-gradient-to-r from-red-600 to-rose-600 text-white font-black text-xs px-3 py-1.5 rounded-xl shadow-lg animate-pulse">
                  <Zap className="w-4 h-4 fill-current text-yellow-300" />
                  <span>FLASH SALE ĐANG DIỄN RA -{discountPercent}%</span>
                </div>
              ) : (
                discountPercent > 0 && (
                  <Badge variant="danger" className="text-sm px-3 py-1 font-black shadow-md">
                    Giảm {discountPercent}%
                  </Badge>
                )
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
          <div
            className={`p-5 rounded-2xl border space-y-2 ${
              hasFlashSale
                ? "bg-rose-50/50 dark:bg-rose-950/20 border-rose-200 dark:border-rose-900/60 shadow-lg shadow-rose-500/5"
                : "bg-blue-50/50 dark:bg-slate-900 border-blue-100 dark:border-slate-800"
            }`}
          >
            {hasFlashSale && (
              <div className="flex items-center justify-between text-xs font-bold text-rose-600 dark:text-rose-400 pb-1 border-b border-rose-100 dark:border-rose-900/40">
                <span className="flex items-center gap-1">
                  <Zap className="w-3.5 h-3.5 fill-current" /> GIÁ ƯU ĐÃI FLASH SALE
                </span>
                {offer?.remaining_stock !== undefined && (
                  <span>Còn {offer.remaining_stock} suất giá sốc</span>
                )}
              </div>
            )}
            <div className="flex items-baseline gap-3 pt-1">
              <span
                className={`text-3xl sm:text-4xl font-black ${
                  hasFlashSale
                    ? "text-rose-600 dark:text-rose-400"
                    : "text-blue-600 dark:text-blue-400"
                }`}
              >
                {formatPrice(currentPrice)}
              </span>
              {originalPrice > currentPrice && (
                <span className="text-base text-slate-400 line-through">
                  {formatPrice(originalPrice)}
                </span>
              )}
            </div>
            <div className="flex items-center justify-between gap-2 pt-1 border-t border-blue-100 dark:border-slate-800">
              <div className="flex items-center gap-2 text-xs font-medium text-emerald-600 dark:text-emerald-400">
                <CheckCircle2 className="w-4 h-4" />
                <span>Giá đã bao gồm VAT và bảo hành chính hãng</span>
              </div>
              <div
                className={`text-xs font-bold px-2.5 py-1 rounded-full ${
                  hasFlashSale
                    ? "bg-rose-500/10 text-rose-600 dark:text-rose-400 border border-rose-500/20"
                    : product.stock > 5
                    ? "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 border border-emerald-500/20"
                    : product.stock > 0
                    ? "bg-amber-500/10 text-amber-600 dark:text-amber-400 border border-amber-500/20"
                    : "bg-rose-500/10 text-rose-600 dark:text-rose-400 border border-rose-500/20"
                }`}
              >
                {hasFlashSale && offer?.remaining_stock !== undefined
                  ? `Flash Sale: Còn ${offer.remaining_stock} suất`
                  : product.stock > 0
                  ? `Tồn kho: ${product.stock} chiếc`
                  : "Tạm hết hàng"}
              </div>
            </div>
          </div>

          {/* Add to Cart Client Action */}
          <AddToCartButton product={product} initialOffer={offer} />

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
