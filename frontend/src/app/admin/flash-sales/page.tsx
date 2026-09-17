"use client";

import { Button } from "@/components/ui/button";
import { AddItemModal } from "@/features/flash-sale/components/admin/add-item-modal";
import { CreateCampaignModal } from "@/features/flash-sale/components/admin/create-campaign-modal";
import { EditCampaignModal } from "@/features/flash-sale/components/admin/edit-campaign-modal";
import { EditItemModal } from "@/features/flash-sale/components/admin/edit-item-modal";
import { flashSaleService } from "@/features/flash-sale/services/flash-sale-service";
import { AdminCampaign, AdminCampaignItem } from "@/features/flash-sale/types";
import { formatPrice } from "@/lib/utils";
import {
  AlertTriangle,
  Calendar,
  CheckCircle2,
  Clock,
  Copy,
  Flame,
  Layers,
  Loader2,
  Package,
  Pencil,
  Plus,
  RefreshCw,
  StopCircle,
  Trash2,
  Zap,
} from "lucide-react";
import Link from "next/link";
import React, { useEffect, useState } from "react";

export default function AdminFlashSalesPage() {
  // Page Core State
  const [campaigns, setCampaigns] = useState<AdminCampaign[]>([]);
  const [loading, setLoading] = useState(true);
  const [filterStatus, setFilterStatus] = useState<string>("");
  const [actionLoadingId, setActionLoadingId] = useState<number | null>(null);
  const [toastMessage, setToastMessage] = useState<{ text: string; type: "success" | "error" } | null>(null);

  // Modal Control Context States
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [editingCampaign, setEditingCampaign] = useState<AdminCampaign | null>(null);
  const [editingItemContext, setEditingItemContext] = useState<{
    campaignId: number;
    item: AdminCampaignItem;
  } | null>(null);
  const [addingItemCampaignId, setAddingItemCampaignId] = useState<number | null>(null);

  // Toast notification helper
  const showToast = (text: string, type: "success" | "error") => {
    setToastMessage({ text, type });
    setTimeout(() => {
      setToastMessage(null);
    }, 4000);
  };

  // Load campaigns
  const loadCampaigns = async () => {
    try {
      setLoading(true);
      const res = await flashSaleService.listCampaigns(filterStatus || undefined, 1, 50);
      let list: AdminCampaign[] = [];
      if (res && res.data) {
        if (Array.isArray(res.data)) {
          list = res.data;
        } else if (Array.isArray((res.data as any).campaigns)) {
          list = (res.data as any).campaigns;
        }
      }
      setCampaigns(list);
    } catch (err: any) {
      showToast(err?.message || "Không thể tải danh sách chiến dịch Flash Sale", "error");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadCampaigns();
  }, [filterStatus]);

  // 1. Activate Campaign
  const handleActivate = async (campaignId: number) => {
    try {
      setActionLoadingId(campaignId);
      await flashSaleService.activateCampaign(campaignId);
      showToast(`Kích hoạt Saga phân bổ kho chiến dịch #${campaignId} thành công!`, "success");
      await loadCampaigns();
    } catch (err: any) {
      showToast(err?.message || "Kích hoạt chiến dịch thất bại", "error");
    } finally {
      setActionLoadingId(null);
    }
  };

  // 2. Clone Campaign
  const handleClone = async (campaignId: number) => {
    if (!confirm(`Bạn có chắc muốn NHÂN BẢN chiến dịch #${campaignId} thành một bản nháp mới?`)) {
      return;
    }

    try {
      setActionLoadingId(campaignId);
      const res = await flashSaleService.cloneCampaign(campaignId);
      showToast(`Đã nhân bản thành công! Bản sao mới có ID: #${res.data?.id}`, "success");
      await loadCampaigns();
    } catch (err: any) {
      showToast(err?.message || "Nhân bản chiến dịch thất bại", "error");
    } finally {
      setActionLoadingId(null);
    }
  };

  // 3. End Campaign (Hoàn kho)
  const handleEnd = async (campaignId: number) => {
    if (!confirm(`Bạn có chắc muốn KẾT THÚC chiến dịch #${campaignId}? Toàn bộ số lượng chưa bán hết sẽ được hoàn trả về kho thường.`)) {
      return;
    }

    try {
      setActionLoadingId(campaignId);
      await flashSaleService.endCampaign(campaignId);
      showToast(`Chiến dịch #${campaignId} đã kết thúc và kho đã được hoàn trả.`, "success");
      await loadCampaigns();
    } catch (err: any) {
      showToast(err?.message || "Kết thúc chiến dịch thất bại", "error");
    } finally {
      setActionLoadingId(null);
    }
  };

  // 4. Delete Item from Campaign
  const handleDeleteItem = async (campId: number, itemId: number, productId: number) => {
    if (!confirm(`Bạn có chắc muốn XÓA sản phẩm #${productId} khỏi chiến dịch #${campId}?`)) {
      return;
    }

    try {
      setActionLoadingId(campId);
      await flashSaleService.deleteCampaignItem(campId, itemId);
      showToast(`Đã xóa sản phẩm #${productId} khỏi chiến dịch #${campId}!`, "success");
      await loadCampaigns();
    } catch (err: any) {
      showToast(err?.message || "Xóa sản phẩm thất bại", "error");
    } finally {
      setActionLoadingId(null);
    }
  };

  // Stats calculation
  const campaignList = Array.isArray(campaigns) ? campaigns : [];
  const totalActive = campaignList.filter((c) => c?.status === "ACTIVE").length;
  const totalDraft = campaignList.filter((c) => c?.status === "DRAFT" || c?.status === "ALLOCATING").length;
  const totalEnded = campaignList.filter((c) => c?.status === "ENDED").length;

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 p-4 sm:p-8">
      {/* Toast Notification */}
      {toastMessage && (
        <div
          className={`fixed top-5 right-5 z-50 px-4 py-3 rounded-2xl shadow-2xl border text-sm font-bold flex items-center gap-2.5 animate-in slide-in-from-top-4 ${
            toastMessage.type === "success"
              ? "bg-emerald-950/90 border-emerald-500 text-emerald-300"
              : "bg-rose-950/90 border-rose-500 text-rose-300"
          }`}
        >
          {toastMessage.type === "success" ? (
            <CheckCircle2 className="w-5 h-5 text-emerald-400" />
          ) : (
            <AlertTriangle className="w-5 h-5 text-rose-400" />
          )}
          <span>{toastMessage.text}</span>
        </div>
      )}

      <div className="max-w-7xl mx-auto space-y-8">
        {/* Top Header & Breadcrumbs */}
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 pb-6 border-b border-slate-800">
          <div>
            <div className="flex items-center gap-2 text-xs text-slate-400 font-medium mb-1">
              <Link href="/" className="hover:text-slate-200 transition-colors">
                Trang chủ
              </Link>
              <span>/</span>
              <span className="text-amber-400 font-bold">Admin Flash Sale Engine</span>
            </div>
            <h1 className="text-2xl sm:text-3xl font-black text-white tracking-tight flex items-center gap-3">
              <Flame className="w-8 h-8 text-amber-500 fill-amber-500 animate-pulse" />
              QUẢN TRỊ CHIẾN DỊCH FLASH SALE
            </h1>
            <p className="text-xs sm:text-sm text-slate-400 mt-1">
              Điều phối chiến dịch phân tán, Saga cấp phát tồn kho và giám sát hạn mức mua
            </p>
          </div>

          <div className="flex items-center gap-3">
            <Button
              variant="outline"
              onClick={loadCampaigns}
              disabled={loading}
              className="border-slate-700 bg-slate-900 hover:bg-slate-800 text-slate-200 gap-2 h-11 rounded-xl"
            >
              <RefreshCw className={`w-4 h-4 ${loading ? "animate-spin" : ""}`} />
              Làm mới
            </Button>

            <Button
              onClick={() => setShowCreateModal(true)}
              className="bg-gradient-to-r from-amber-500 via-rose-600 to-red-600 hover:from-amber-600 hover:to-red-700 text-white font-black text-sm h-11 px-5 rounded-xl shadow-lg shadow-rose-600/20 gap-2"
            >
              <Plus className="w-5 h-5" />
              TẠO CHIẾN DỊCH MỚI
            </Button>
          </div>
        </div>

        {/* Stats Dashboard Grid */}
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
          <div className="p-5 rounded-2xl bg-gradient-to-br from-emerald-950/40 to-slate-900 border border-emerald-500/30 flex items-center justify-between shadow-lg">
            <div>
              <p className="text-xs font-bold text-emerald-400 uppercase tracking-wider">Đang Mở Bán (ACTIVE)</p>
              <h3 className="text-3xl font-black text-white mt-1 font-mono">{totalActive}</h3>
              <p className="text-[11px] text-slate-400 mt-1">Khách hàng đang tranh mua</p>
            </div>
            <div className="p-3 bg-emerald-500/20 rounded-2xl text-emerald-400">
              <Zap className="w-7 h-7" />
            </div>
          </div>

          <div className="p-5 rounded-2xl bg-gradient-to-br from-amber-950/40 to-slate-900 border border-amber-500/30 flex items-center justify-between shadow-lg">
            <div>
              <p className="text-xs font-bold text-amber-400 uppercase tracking-wider">Bản Nháp (DRAFT)</p>
              <h3 className="text-3xl font-black text-white mt-1 font-mono">{totalDraft}</h3>
              <p className="text-[11px] text-slate-400 mt-1">Chờ kích hoạt Saga phân bổ kho</p>
            </div>
            <div className="p-3 bg-amber-500/20 rounded-2xl text-amber-400">
              <Layers className="w-7 h-7" />
            </div>
          </div>

          <div className="p-5 rounded-2xl bg-gradient-to-br from-slate-900 to-slate-950 border border-slate-800 flex items-center justify-between shadow-lg">
            <div>
              <p className="text-xs font-bold text-slate-400 uppercase tracking-wider">Đã Kết Thúc (ENDED)</p>
              <h3 className="text-3xl font-black text-white mt-1 font-mono">{totalEnded}</h3>
              <p className="text-[11px] text-slate-400 mt-1">Đã hoàn trả tồn kho thừa</p>
            </div>
            <div className="p-3 bg-slate-800 rounded-2xl text-slate-400">
              <Clock className="w-7 h-7" />
            </div>
          </div>
        </div>

        {/* Filters bar */}
        <div className="flex items-center gap-2 overflow-x-auto pb-2 border-b border-slate-800/80">
          {[
            { id: "", label: "Tất cả chiến dịch" },
            { id: "ACTIVE", label: "🟢 Đang mở bán (ACTIVE)" },
            { id: "DRAFT", label: "🟡 Bản nháp (DRAFT)" },
            { id: "ENDED", label: "⚪ Đã kết thúc (ENDED)" },
          ].map((tab) => (
            <button
              key={tab.id}
              onClick={() => setFilterStatus(tab.id)}
              className={`px-4 py-2 rounded-xl text-xs font-bold transition-all whitespace-nowrap ${
                filterStatus === tab.id
                  ? "bg-rose-600 text-white shadow-md shadow-rose-600/30"
                  : "bg-slate-900 text-slate-400 hover:text-slate-200 hover:bg-slate-800"
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {/* Campaign List */}
        {loading ? (
          <div className="py-24 text-center space-y-3">
            <Loader2 className="w-10 h-10 text-amber-500 animate-spin mx-auto" />
            <p className="text-sm text-slate-400">Đang tải danh sách chiến dịch Flash Sale...</p>
          </div>
        ) : campaignList.length === 0 ? (
          <div className="py-20 text-center rounded-3xl bg-slate-900/60 border border-slate-800 space-y-4">
            <Package className="w-12 h-12 text-slate-600 mx-auto" />
            <div className="space-y-1">
              <h3 className="text-base font-bold text-slate-300">Chưa có chiến dịch Flash Sale nào</h3>
              <p className="text-xs text-slate-500">Bấm nút &quot;Tạo Chiến Dịch Mới&quot; để thiết lập khung giờ vàng giá sốc</p>
            </div>
            <Button
              onClick={() => setShowCreateModal(true)}
              className="bg-amber-500 hover:bg-amber-600 text-slate-950 font-black text-xs h-9 rounded-xl"
            >
              Tạo Chiến Dịch Đầu Tiên
            </Button>
          </div>
        ) : (
          <div className="space-y-6">
            {campaignList.map((camp) => {
              const isActionLoading = actionLoadingId === camp.id;
              const isDraft = camp.status === "DRAFT";
              const isActive = camp.status === "ACTIVE";

              return (
                <div
                  key={camp.id}
                  className="rounded-3xl bg-slate-900/90 border border-slate-800 hover:border-slate-700 p-6 space-y-6 shadow-xl transition-all"
                >
                  {/* Campaign Header Info */}
                  <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
                    <div className="space-y-1.5">
                      <div className="flex items-center gap-2.5 flex-wrap">
                        <span className="font-mono text-xs font-bold text-slate-400 bg-slate-800 px-2 py-0.5 rounded-md">
                          #{camp.id}
                        </span>
                        <h2 className="text-lg font-black text-white tracking-tight">
                          {camp.name}
                        </h2>

                        {/* Status Badge */}
                        {camp.status === "ACTIVE" && (
                          <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-black bg-emerald-500/20 text-emerald-300 border border-emerald-500/40">
                            <span className="w-2 h-2 rounded-full bg-emerald-400 animate-ping" />
                            ĐANG MỞ BÁN
                          </span>
                        )}
                        {camp.status === "DRAFT" && (
                          <span className="px-2.5 py-0.5 rounded-full text-xs font-bold bg-amber-500/20 text-amber-300 border border-amber-500/40">
                            BẢN NHÁP (CHƯA KÍCH HOẠT)
                          </span>
                        )}
                        {camp.status === "ALLOCATING" && (
                          <span className="px-2.5 py-0.5 rounded-full text-xs font-bold bg-blue-500/20 text-blue-300 border border-blue-500/40 flex items-center gap-1">
                            <Loader2 className="w-3 h-3 animate-spin" />
                            ĐANG SAGA PHÂN BỔ KHO...
                          </span>
                        )}
                        {camp.status === "ENDED" && (
                          <span className="px-2.5 py-0.5 rounded-full text-xs font-bold bg-slate-800 text-slate-400 border border-slate-700">
                            ĐÃ KẾT THÚC
                          </span>
                        )}
                        {camp.status === "ACTIVATION_FAILED" && (
                          <span className="px-2.5 py-0.5 rounded-full text-xs font-bold bg-rose-500/20 text-rose-300 border border-rose-500/40">
                            KÍCH HOẠT THẤT BẠI (ĐÃ BỒI HOÀN KHO)
                          </span>
                        )}
                      </div>
                      {camp.description && (
                        <p className="text-xs text-slate-400">{camp.description}</p>
                      )}
                    </div>

                    {/* Action Buttons Group */}
                    <div className="flex items-center gap-2 flex-wrap">
                      {/* Edit Campaign Info Button */}
                      {(isDraft || camp.status === "ACTIVATION_FAILED") && (
                        <Button
                          size="sm"
                          variant="outline"
                          disabled={isActionLoading}
                          onClick={() => setEditingCampaign(camp)}
                          className="border-slate-700 bg-slate-800/80 hover:bg-slate-700 text-slate-200 text-xs font-bold gap-1.5 h-9 rounded-xl"
                        >
                          <Pencil className="w-3.5 h-3.5 text-blue-400" />
                          Sửa chiến dịch
                        </Button>
                      )}

                      {/* Add Item Button */}
                      {(isDraft || camp.status === "ACTIVATION_FAILED") && (
                        <Button
                          size="sm"
                          variant="outline"
                          disabled={isActionLoading}
                          onClick={() => setAddingItemCampaignId(camp.id)}
                          className="border-slate-700 bg-slate-800/80 hover:bg-slate-700 text-slate-200 text-xs font-bold gap-1.5 h-9 rounded-xl"
                        >
                          <Plus className="w-3.5 h-3.5 text-emerald-400" />
                          + Thêm SP
                        </Button>
                      )}

                      {/* Activate Button */}
                      {(isDraft || camp.status === "ACTIVATION_FAILED") && (
                        <Button
                          size="sm"
                          disabled={isActionLoading}
                          onClick={() => handleActivate(camp.id)}
                          className="bg-emerald-600 hover:bg-emerald-700 text-white font-black text-xs gap-1.5 h-9 rounded-xl shadow-md shadow-emerald-900/40"
                        >
                          {isActionLoading ? (
                            <Loader2 className="w-4 h-4 animate-spin" />
                          ) : (
                            <Zap className="w-4 h-4 fill-current" />
                          )}
                          KÍCH HOẠT SAGA
                        </Button>
                      )}

                      {/* Clone Button */}
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={isActionLoading}
                        onClick={() => handleClone(camp.id)}
                        className="border-slate-700 bg-slate-800/80 hover:bg-slate-700 text-slate-200 text-xs font-bold gap-1.5 h-9 rounded-xl"
                      >
                        <Copy className="w-3.5 h-3.5 text-amber-400" />
                        Nhân bản
                      </Button>

                      {/* End Button */}
                      {isActive && (
                        <Button
                          size="sm"
                          disabled={isActionLoading}
                          onClick={() => handleEnd(camp.id)}
                          className="bg-rose-600/20 border border-rose-500/50 hover:bg-rose-600 text-rose-300 hover:text-white text-xs font-bold gap-1.5 h-9 rounded-xl"
                        >
                          <StopCircle className="w-3.5 h-3.5" />
                          Kết thúc & Trả kho
                        </Button>
                      )}
                    </div>
                  </div>

                  {/* Time Range Information */}
                  <div className="flex items-center gap-4 text-xs text-slate-400 bg-slate-950/60 p-3 rounded-2xl border border-slate-800/80 flex-wrap">
                    <div className="flex items-center gap-1.5">
                      <Calendar className="w-4 h-4 text-amber-400" />
                      <span>Bắt đầu:</span>
                      <strong className="text-slate-200">
                        {new Date(camp.starts_at).toLocaleString("vi-VN")}
                      </strong>
                    </div>
                    <span className="text-slate-600">|</span>
                    <div className="flex items-center gap-1.5">
                      <Clock className="w-4 h-4 text-rose-400" />
                      <span>Kết thúc:</span>
                      <strong className="text-slate-200">
                        {new Date(camp.ends_at).toLocaleString("vi-VN")}
                      </strong>
                    </div>
                  </div>

                  {/* Campaign Items Table */}
                  <div className="space-y-2">
                    <div className="flex items-center justify-between">
                      <h4 className="text-xs font-bold text-slate-400 uppercase tracking-wider flex items-center gap-1.5">
                        <Package className="w-4 h-4 text-slate-400" />
                        Danh sách sản phẩm trong chiến dịch ({camp.items?.length || 0})
                      </h4>
                      {(isDraft || camp.status === "ACTIVATION_FAILED") && (
                        <button
                          type="button"
                          onClick={() => setAddingItemCampaignId(camp.id)}
                          className="text-xs font-bold text-amber-400 hover:text-amber-300 flex items-center gap-1 transition-colors"
                        >
                          <Plus className="w-3.5 h-3.5" />
                          Thêm sản phẩm
                        </button>
                      )}
                    </div>

                    {camp.items && camp.items.length > 0 ? (
                      <div className="overflow-x-auto rounded-2xl border border-slate-800">
                        <table className="w-full text-left text-xs">
                          <thead className="bg-slate-950 text-slate-400 font-bold border-b border-slate-800">
                            <tr>
                              <th className="p-3">Mã SP</th>
                              <th className="p-3">Giá Sale</th>
                              <th className="p-3">Giá Gốc</th>
                              <th className="p-3">Kho Cấp Phát</th>
                              <th className="p-3">Đã Bán</th>
                              <th className="p-3">Loại Hạn Mức Quota</th>
                              <th className="p-3">Giữ Chỗ</th>
                              {(isDraft || camp.status === "ACTIVATION_FAILED") && (
                                <th className="p-3 text-right">Thao tác</th>
                              )}
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-slate-800/60">
                            {camp.items.map((it) => (
                              <tr key={it.id} className="hover:bg-slate-800/40">
                                <td className="p-3 font-mono font-bold text-amber-400">
                                  #{it.product_id}
                                </td>
                                <td className="p-3 font-bold text-rose-400 font-mono">
                                  {formatPrice(it.sale_price)}
                                </td>
                                <td className="p-3 text-slate-400 line-through font-mono">
                                  {formatPrice(it.original_price)}
                                </td>
                                <td className="p-3 font-mono font-bold text-white">
                                  {it.allocated_stock} suất
                                </td>
                                <td className="p-3 font-mono font-bold text-amber-300">
                                  {it.sold_stock} suất
                                </td>
                                <td className="p-3">
                                  {it.max_quantity_per_user === 1 ? (
                                    <span className="px-2 py-0.5 rounded-md bg-rose-500/20 text-rose-300 border border-rose-500/30 font-bold text-[11px]">
                                      Loại 1: Deal Sốc (1 lần duy nhất)
                                    </span>
                                  ) : (
                                    <span className="px-2 py-0.5 rounded-md bg-emerald-500/20 text-emerald-300 border border-emerald-500/30 font-bold text-[11px]">
                                      Loại 2: Mua nhiều lần (Max {it.max_quantity_per_user})
                                    </span>
                                  )}
                                </td>
                                <td className="p-3 text-slate-400 font-mono">
                                  {it.reservation_seconds || 120}s
                                </td>
                                {(isDraft || camp.status === "ACTIVATION_FAILED") && (
                                  <td className="p-3 text-right">
                                    <div className="flex items-center justify-end gap-1.5">
                                      <button
                                        type="button"
                                        onClick={() => setEditingItemContext({ campaignId: camp.id, item: it })}
                                        className="p-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-blue-400 hover:text-blue-300 transition-colors"
                                        title="Chỉnh sửa giá & tồn kho"
                                      >
                                        <Pencil className="w-3.5 h-3.5" />
                                      </button>
                                      <button
                                        type="button"
                                        onClick={() => handleDeleteItem(camp.id, it.id, it.product_id)}
                                        className="p-1.5 rounded-lg bg-slate-800 hover:bg-rose-950/80 text-rose-400 hover:text-rose-300 transition-colors"
                                        title="Xóa sản phẩm"
                                      >
                                        <Trash2 className="w-3.5 h-3.5" />
                                      </button>
                                    </div>
                                  </td>
                                )}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    ) : (
                      <p className="text-xs text-slate-500 italic p-3 bg-slate-950/40 rounded-xl">
                        Chưa có sản phẩm nào được gắn vào chiến dịch này.
                      </p>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Modular Modals (Isolated Re-render & Auto Cleanup) */}
      <CreateCampaignModal
        isOpen={showCreateModal}
        onClose={() => setShowCreateModal(false)}
        onSuccess={(msg) => {
          showToast(msg, "success");
          loadCampaigns();
        }}
        onError={(err) => showToast(err, "error")}
      />

      <EditCampaignModal
        campaign={editingCampaign}
        onClose={() => setEditingCampaign(null)}
        onSuccess={(msg) => {
          showToast(msg, "success");
          loadCampaigns();
        }}
        onError={(err) => showToast(err, "error")}
      />

      <EditItemModal
        data={editingItemContext}
        onClose={() => setEditingItemContext(null)}
        onSuccess={(msg) => {
          showToast(msg, "success");
          loadCampaigns();
        }}
        onError={(err) => showToast(err, "error")}
      />

      <AddItemModal
        campaignId={addingItemCampaignId}
        onClose={() => setAddingItemCampaignId(null)}
        onSuccess={(msg) => {
          showToast(msg, "success");
          loadCampaigns();
        }}
        onError={(err) => showToast(err, "error")}
      />
    </div>
  );
}
