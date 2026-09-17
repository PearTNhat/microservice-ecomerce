"use client";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { flashSaleService } from "@/features/flash-sale/services/flash-sale-service";
import { Loader2, Plus, ShieldCheck, Users, X } from "lucide-react";
import React, { useState } from "react";

interface AddItemModalProps {
  campaignId: number | null;
  onClose: () => void;
  onSuccess: (message: string) => void;
  onError: (error: string) => void;
}

export function AddItemModal({
  campaignId,
  onClose,
  onSuccess,
  onError,
}: AddItemModalProps) {
  const [productId, setProductId] = useState("");
  const [salePrice, setSalePrice] = useState("");
  const [originalPrice, setOriginalPrice] = useState("");
  const [allocatedStock, setAllocatedStock] = useState("10");
  const [quotaType, setQuotaType] = useState<"SINGLE" | "MULTIPLE">("SINGLE");
  const [maxUser, setMaxUser] = useState("2");
  const [resvSec, setResvSec] = useState("120");
  const [submitting, setSubmitting] = useState(false);

  if (!campaignId) return null;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      setSubmitting(true);
      const maxUserNum = quotaType === "SINGLE" ? 1 : Math.max(1, parseInt(maxUser) || 2);
      await flashSaleService.addCampaignItem(campaignId, {
        product_id: parseInt(productId) || 1,
        sale_price: parseFloat(salePrice) || 0,
        original_price: parseFloat(originalPrice) || 0,
        allocated_stock: parseInt(allocatedStock) || 10,
        max_quantity_per_user: maxUserNum,
        max_quantity_per_order: 1,
        reservation_seconds: parseInt(resvSec) || 120,
      });

      onSuccess(`Thêm sản phẩm #${productId} vào chiến dịch #${campaignId} thành công!`);
      onClose();
    } catch (err: any) {
      onError(err?.message || "Thêm sản phẩm thất bại");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/80 backdrop-blur-sm animate-in fade-in duration-200">
      <div className="relative w-full max-w-lg bg-slate-900 border border-emerald-500/30 rounded-3xl shadow-2xl overflow-hidden text-slate-100 flex flex-col max-h-[90vh]">
        <div className="p-5 bg-gradient-to-r from-emerald-600 to-teal-600 text-white flex items-center justify-between">
          <div className="flex items-center gap-2.5">
            <div className="p-2 bg-white/20 rounded-xl">
              <Plus className="w-5 h-5 text-white" />
            </div>
            <div>
              <h3 className="text-lg font-black tracking-tight">Thêm Sản Phẩm Vào Chiến Dịch #{campaignId}</h3>
              <p className="text-xs text-emerald-100">Bổ sung mặt hàng tham gia chương trình flash sale</p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 rounded-full hover:bg-white/20 transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="p-6 overflow-y-auto space-y-4 text-xs">
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="text-slate-300 font-semibold block mb-1">Mã sản phẩm (Product ID) *</label>
              <Input
                required
                type="number"
                min="1"
                placeholder="VD: 5"
                value={productId}
                onChange={(e) => setProductId(e.target.value)}
                className="bg-slate-800 border-slate-700 text-white h-10 text-sm font-mono"
              />
            </div>
            <div>
              <label className="text-slate-300 font-semibold block mb-1">Số lượng phân bổ (Suất bán) *</label>
              <Input
                required
                type="number"
                min="1"
                value={allocatedStock}
                onChange={(e) => setAllocatedStock(e.target.value)}
                className="bg-slate-800 border-slate-700 text-white h-10 text-sm font-mono"
              />
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="text-slate-300 font-semibold block mb-1">Giá gốc (VNĐ) *</label>
              <Input
                required
                type="number"
                min="1000"
                placeholder="VD: 1000000"
                value={originalPrice}
                onChange={(e) => setOriginalPrice(e.target.value)}
                className="bg-slate-800 border-slate-700 text-white h-10 text-sm font-mono"
              />
            </div>
            <div>
              <label className="text-slate-300 font-semibold block mb-1">Giá Flash Sale (VNĐ) *</label>
              <Input
                required
                type="number"
                min="1000"
                placeholder="VD: 499000"
                value={salePrice}
                onChange={(e) => setSalePrice(e.target.value)}
                className="bg-slate-800 border-slate-700 text-white h-10 text-sm font-mono text-rose-400 font-bold"
              />
            </div>
          </div>

          <div>
            <label className="text-slate-300 font-semibold block mb-1">Thời gian giữ chỗ (Giây) *</label>
            <Input
              required
              type="number"
              min="30"
              max="600"
              value={resvSec}
              onChange={(e) => setResvSec(e.target.value)}
              className="bg-slate-800 border-slate-700 text-white h-10 text-sm font-mono"
            />
          </div>

          {/* Quota Type Selection */}
          <div className="space-y-2 pt-2">
            <label className="text-slate-300 font-semibold block">Hạn mức người dùng *</label>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
              <button
                type="button"
                onClick={() => setQuotaType("SINGLE")}
                className={`p-3 rounded-2xl border text-left space-y-1 transition-all ${
                  quotaType === "SINGLE"
                    ? "bg-rose-600/20 border-rose-500 text-rose-300 shadow-md shadow-rose-950"
                    : "bg-slate-800/60 border-slate-700 text-slate-400 hover:text-slate-200"
                }`}
              >
                <div className="font-black text-xs flex items-center gap-1.5">
                  <ShieldCheck className="w-4 h-4 text-rose-400" />
                  Loại 1: Deal Sốc (1 suất)
                </div>
                <p className="text-[10px] text-slate-400">Mỗi khách chỉ mua được 1 lần / 1 suất.</p>
              </button>

              <button
                type="button"
                onClick={() => setQuotaType("MULTIPLE")}
                className={`p-3 rounded-2xl border text-left space-y-1 transition-all ${
                  quotaType === "MULTIPLE"
                    ? "bg-emerald-600/20 border-emerald-500 text-emerald-300 shadow-md shadow-emerald-950"
                    : "bg-slate-800/60 border-slate-700 text-slate-400 hover:text-slate-200"
                }`}
              >
                <div className="font-black text-xs flex items-center gap-1.5">
                  <Users className="w-4 h-4 text-emerald-400" />
                  Loại 2: Xả Kho (Nhiều suất)
                </div>
                <p className="text-[10px] text-slate-400">Khách được mua tích lũy qua nhiều đơn.</p>
              </button>
            </div>

            {quotaType === "MULTIPLE" && (
              <div className="p-3 bg-slate-800/80 rounded-xl border border-slate-700 space-y-1 animate-in fade-in">
                <label className="text-slate-300 font-semibold block">Hạn mức tối đa/khách:</label>
                <Input
                  type="number"
                  min="2"
                  max="100"
                  value={maxUser}
                  onChange={(e) => setMaxUser(e.target.value)}
                  className="bg-slate-900 border-slate-600 text-white h-9 font-mono"
                />
              </div>
            )}
          </div>

          <div className="flex gap-3 pt-3">
            <Button
              type="submit"
              disabled={submitting}
              className="flex-1 bg-emerald-600 hover:bg-emerald-700 text-white font-black text-xs h-11 rounded-xl shadow-lg shadow-emerald-600/20"
            >
              {submitting ? (
                <>
                  <Loader2 className="w-4 h-4 animate-spin mr-2" />
                  Đang thêm sản phẩm...
                </>
              ) : (
                "THÊM SẢN PHẨM VÀO CHIẾN DỊCH"
              )}
            </Button>
            <Button
              type="button"
              variant="outline"
              onClick={onClose}
              className="border-slate-700 text-slate-300 hover:bg-slate-800 h-11 rounded-xl"
            >
              Hủy
            </Button>
          </div>
        </form>
      </div>
    </div>
  );
}
