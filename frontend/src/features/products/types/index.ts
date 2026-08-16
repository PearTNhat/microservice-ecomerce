export interface Category {
  id: number;
  name: string;
  slug: string;
  icon?: string;
  parent_id?: number;
  children?: Category[];
}

export interface Brand {
  id: number;
  name: string;
  slug: string;
  logo?: string;
}

export interface Product {
  id: number;
  name: string;
  slug: string;
  description?: string;
  price: number;
  discount_price?: number;
  stock: number;
  thumbnail?: string;
  images?: string[];
  specifications?: Record<string, any>;
  category?: Category;
  brand?: Brand;
  rating: number;
  views: number;
}

export interface ProductFilterParams {
  category_id?: number;
  brand_id?: number;
  min_price?: number;
  max_price?: number;
  keyword?: string;
  sort_by?: "price_asc" | "price_desc" | "newest" | "views";
  page?: number;
  limit?: number;
}

export interface PaginatedProducts {
  total: number;
  page: number;
  limit: number;
  total_pages: number;
  products: Product[];
}
