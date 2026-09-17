"use client";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { flashSaleService } from "@/features/flash-sale/services/flash-sale-service";
import { Flame, Loader2, Package, ShieldCheck, Tag, Users, X } from "lucide-react";
import React, { useState } from "react";

interface CreateCampaignModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSuccess: (message: string) => void;
  onError: (error: string) => void;
}

export function CreateCampaignModal({
  isOpen,
  onClose,
  onSuccess,
  onError,
}: CreateCampaignModalProps) {
  // Campaign form state
  const [formName, setFormName] = useState("");
  const [formDesc, setFormDesc] = useState("");
  const [formStartsAt, setFormStartsAt] = useState("");
  const [formEndsAt, setFormEndsAt] = useState("");

  // Initial item state
  const [itemProductId, setItemProductId] = useState("1");
  const [itemSalePrice, setItemSalePrice] = useState("500000");
  const [itemOriginalPrice, setItemOriginalPrice] = useState("1000000");
  const [itemAllocatedStock, setItemAllocatedStock] = useState("10");
  const [itemQuotaType, setItemQuotaType] = useState<"SINGLE" | "MULTIPLE">("SINGLE");
  const [itemMaxUser, setItemMaxUser] = useState("2");
  const [submitting, setSubmitting] = useState(false);

  if (!isOpen) return null;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!formName || !formStartsAt || !formEndsAt) {
      alert("Vui lòng điền đầy đủ thông tin thời gian và tên chiến dịch");
      return;
    }

    try {
      setSubmitting(true);
      // Bước 1: Tạo Campaign
      const campRes = await flashSaleService.createCampaign({
        name: formName,
        description: formDesc,
        starts_at: new Date(formStartsAt).toISOString(),
        ends_at: new Date(formEndsAt).toISOString(),
      });

      const newCampId = campRes.data.id;

      // Bước 2: Thêm Item vào Campaign
      const maxUser = itemQuotaType === "SINGLE" ? 1 : Math.max(1, parseInt(itemMaxUser) || 2);
      await flashSaleService.addCampaignItem(newCampId, {
        product_id: parseInt(itemProductId) || 1,
        sale_price: parseFloat(itemSalePrice) || 500000,
        original_price: parseFloat(itemOriginalPrice) || 1000000,
        allocated_stock: parseInt(itemAllocatedStock) || 10,
        max_quantity_per_user: maxUser,
        max_quantity_per_order: 1,
        reservation_seconds: 120,
      });

      onSuccess(`Tạo chiến dịch #${newCampId} và gắn sản phẩm thành công!`);
      onClose();
    } catch (err: any) {
      onError(err?.message || "Tạo chiến dịch thất bại");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/80 backdrop-blur-sm animate-in fade-in duration-200">
      <div className="relative w-full max-w-xl bg-slate-900 border border-amber-500/30 rounded-3xl shadow-2xl overflow-hidden text-slate-100 flex flex-col max-h-[90vh]">
        {/* Modal Header */}
        <div className="p-5 bg-gradient-to-r from-amber-600 via-rose-600 to-red-600 text-white flex items-center justify-between">
          <div className="flex items-center gap-2.5">
            <div className="p-2 bg-white/20 rounded-xl">
              <Flame className="w-5 h-5 text-yellow-300 fill-yellow-300" />
            </div>
            <div>
              <h3 className="text-lg font-black tracking-tight">Tạo Chiến Dịch Flash Sale Mới</h3>
              <p className="text-xs text-amber-100">Cấu hình thời gian, sản phẩm và loại hạn mức người dùng</p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 rounded-full hover:bg-white/20 transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Modal Form */}
        <form onSubmit={handleSubmit} className="p-6 overflow-y-auto space-y-5 text-xs">
          {/* Campaign Info */}
          <div className="space-y-3">
            <h4 className="font-black text-amber-400 uppercase tracking-wider text-[11px] flex items-center gap-1.5">
              <Tag className="w-3.5 h-3.5" />
              1. Thông tin chiến dịch
            </h4>
            <div>
              <label className="text-slate-300 font-semibold block mb-1">Tên chiến dịch *</label>
              <Input
                required
                placeholder="VD: Flash Sale Giờ Vàng 20h"
                value={formName}
                onChange={(e) => setFormName(e.target.value)}
                className="bg-slate-800 border-slate-700 text-white h-10 text-sm"
              />
            </div>
            <div>
              <label className="text-slate-300 font-semibold block mb-1">Mô tả ngắn</label>
              <Input
                placeholder="VD: Giảm giá sốc đến 50% thiết bị điện máy gia dụng"
                value={formDesc}
                onChange={(e) => setFormDesc(e.target.value)}
                className="bg-slate-800 border-slate-700 text-white h-10 text-sm"
              />
            </div>

            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              <div>
                <label className="text-slate-300 font-semibold block mb-1">Thời gian bắt đầu *</label>
                <Input
                  required
                  type="datetime-local"
                  value={formStartsAt}
                  onChange={(e) => setFormStartsAt(e.target.value)}
                  className="bg-slate-800 border-slate-700 text-white h-10 text-xs font-mono"
                />
              </div>
              <div>
                <label className="text-slate-300 font-semibold block mb-1">Thời gian kết thúc *</label>
                <Input
                  required
                  type="datetime-local"
                  value={formEndsAt}
                  onChange={(e) => setFormEndsAt(e.target.value)}
                  className="bg-slate-800 border-slate-700 text-white h-10 text-xs font-mono"
                />
              </div>
            </div>
          </div>

          {/* Initial Item Setup */}
          <div className="space-y-3 pt-3 border-t border-slate-800">
            <h4 className="font-black text-amber-400 uppercase tracking-wider text-[11px] flex items-center gap-1.5">
              <Package className="w-3.5 h-3.5" />
              2. Cấu hình sản phẩm Flash Sale
            </h4>

            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="text-slate-300 font-semibold block mb-1">Mã sản phẩm (Product ID) *</label>
                <Input
                  required
                  type="number"
                  min="1"
                  value={itemProductId}
                  onChange={(e) => setItemProductId(e.target.value)}
                  className="bg-slate-800 border-slate-700 text-white h-10 text-sm font-mono"
                />
              </div>
              <div>
                <label className="text-slate-300 font-semibold block mb-1">Số lượng phân bổ (Suất bán) *</label>
                <Input
                  required
                  type="number"
                  min="1"
                  value={itemAllocatedStock}
                  onChange={(e) => setItemAllocatedStock(e.target.value)}
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
                  value={itemOriginalPrice}
                  onChange={(e) => setItemOriginalPrice(e.target.value)}
                  className="bg-slate-800 border-slate-700 text-white h-10 text-sm font-mono"
                />
              </div>
              <div>
                <label className="text-slate-300 font-semibold block mb-1">Giá Flash Sale (VNĐ) *</label>
                <Input
                  required
                  type="number"
                  min="1000"
                  value={itemSalePrice}
                  onChange={(e) => setItemSalePrice(e.target.value)}
                  className="bg-slate-800 border-slate-700 text-white h-10 text-sm font-mono text-rose-400 font-bold"
                />
              </div>
            </div>

            {/* Quota Type Selection (Loại 1 vs Loại 2) */}
            <div className="space-y-2 pt-2">
              <label className="text-slate-300 font-semibold block">
                Quy định hạn mức mua (Chống bot / Quản lý xả kho) *
              </label>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                <button
                  type="button"
                  onClick={() => setItemQuotaType("SINGLE")}
                  className={`p-3 rounded-2xl border text-left space-y-1 transition-all ${
                    itemQuotaType === "SINGLE"
                      ? "bg-rose-600/20 border-rose-500 text-rose-300 shadow-md shadow-rose-950"
                      : "bg-slate-800/60 border-slate-700 text-slate-400 hover:text-slate-200"
                  }`}
                >
                  <div className="font-black text-xs flex items-center gap-1.5">
                    <ShieldCheck className="w-4 h-4 text-rose-400" />
                    Loại 1: Deal Sốc (1 lần duy nhất)
                  </div>
                  <p className="text-[10px] text-slate-400">
                    Mỗi tài khoản chỉ được mua đúng 1 lần / 1 suất. Chống đầu cơ tích trữ.
                  </p>
                </button>

                <button
                  type="button"
                  onClick={() => setItemQuotaType("MULTIPLE")}
                  className={`p-3 rounded-2xl border text-left space-y-1 transition-all ${
                    itemQuotaType === "MULTIPLE"
                      ? "bg-emerald-600/20 border-emerald-500 text-emerald-300 shadow-md shadow-emerald-950"
                      : "bg-slate-800/60 border-slate-700 text-slate-400 hover:text-slate-200"
                  }`}
                >
                  <div className="font-black text-xs flex items-center gap-1.5">
                    <Users className="w-4 h-4 text-emerald-400" />
                    Loại 2: Xả Kho (Mua nhiều lần)
                  </div>
                  <p className="text-[10px] text-slate-400">
                    Khách được mua nhiều lần qua nhiều đơn, miễn tổng số lượng ≤ Hạn mức.
                  </p>
                </button>
              </div>

              {itemQuotaType === "MULTIPLE" && (
                <div className="p-3 bg-slate-800/80 rounded-xl border border-slate-700 space-y-1 animate-in fade-in">
                  <label className="text-slate-300 font-semibold block">
                    Số lượng tối đa 1 khách được mua (Tích lũy):
                  </label>
                  <Input
                    type="number"
                    min="2"
                    max="100"
                    value={itemMaxUser}
                    onChange={(e) => setItemMaxUser(e.target.value)}
                    className="bg-slate-900 border-slate-600 text-white h-9 font-mono"
                  />
                </div>
              )}
            </div>
          </div>

          {/* Submit Buttons */}
          <div className="flex gap-3 pt-3">
            <Button
              type="submit"
              disabled={submitting}
              className="flex-1 bg-gradient-to-r from-amber-500 to-rose-600 hover:from-amber-600 hover:to-rose-700 text-white font-black text-xs h-11 rounded-xl shadow-lg shadow-rose-600/20"
            >
              {submitting ? (
                <>
                  <Loader2 className="w-4 h-4 animate-spin mr-2" />
                  Đang khởi tạo chiến dịch...
                </>
              ) : (
                "LƯU VÀ TẠO BẢN NHÁP CHIẾN DỊCH"
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
