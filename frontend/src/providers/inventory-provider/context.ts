export interface IInventoryItem { 
    id: string; name: string; 
    quantity: number;
    unit_price: number; 
}

export interface IInventoryState { 
    items: IInventoryItem[]; 
    loading: boolean; 
    error?: string;
 }
