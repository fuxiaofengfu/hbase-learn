package com.hbase.util;

import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class HBasePreRegionUtil {

	public static final String ROWKEY_DESIGN_1 = "1,2,3,4,5,6,7,8,9,A,B,C,D,E,F,G,H,J,L,M,N,O,Q,T,U,V,W,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z";
	public static final String ROWKEY_DESIGN_2 = "0,1,2,3,4,5,6,7,8,9,A,B,C,D,E,F,G,H,J,L,M,N,P,R,S,T,U,V,W,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z";
	public static final String ROWKEY_DESIGN_3 = "0,1,2,3,4,5,6,7,8,9,A,B,C,D,E,F,G,H,K,L,N,P,Q,R,S,V,W,Y,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,x,y,z";
	public static final String ROWKEY_DESIGN_4 = "0,1,2,3,4,5,6,7,8,9,A,B,C,D,E,F,H,I,J,L,M,N,O,P,R,S,T,U,V,W,X,Z,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z";
	public static final String ROWKEY_DESIGN_5 = "0,1,2,3,4,5,6,7,8,9,A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,Q,R,S,T,U,V,W,X,Y,Z,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,t,u,v,w,x,y,z";

	public static List<String> getRegionRowKeys(){

		String[] key1 = ROWKEY_DESIGN_1.split(",");
		String[] key2 = ROWKEY_DESIGN_2.split(",");
		String[] key3 = ROWKEY_DESIGN_3.split(",");
		String[] key4 = ROWKEY_DESIGN_4.split(",");
		String[] key5 = ROWKEY_DESIGN_5.split(",");
		String[][] arr = new String[][]{key1,key2,key3,key4,key5};

		return cartesian(arr);
	}

	/**
	 * 笛卡尔积
	 * @param arr
	 * @return
	 */
	public static List<String> cartesian(String[][] arr){
		if (null == arr || arr.length <= 0){
			return null;
		}
		if (arr.length <= 1){
			return Arrays.asList(new String[]{});
		}
		List<String> result = new ArrayList<>();
		cartesian(arr,result,0);
		return result;
	}

	private static void cartesian(String[][] arr,List<String> result,int columnIndex){
		if(columnIndex >= arr.length){
			return;
		}
		String[] subArr = arr[columnIndex];
		List<String> newList = new ArrayList<>();
		if (0 != columnIndex){
			for (String s : result){
				for(String str : subArr){
					newList.add(s + str);
				}
			}
			result.clear();
			result.addAll(newList);
		}else{
			result.addAll(Arrays.asList(subArr));
		}
		columnIndex++;
		cartesian(arr,result,columnIndex);
	}

	private static String[] aa = { "aa1", "aa2"};
	private static String[] bb = { "bb1", "bb2", "bb3" };
	private static String[] cc = { "cc1", "cc2", "cc3", "cc4" };
	private static String[][] xyz = { aa, bb, cc };
	private static int counterIndex = xyz.length - 1;
	private static int[] counter = { 0, 0, 0 };

	public static void main(String[] args) throws Exception {

//		for (int i = 0; i < aa.length * bb.length * cc.length; i++) {
//			System.out.print(aa[counter[0]]);
//			System.out.print("\t");
//			System.out.print(bb[counter[1]]);
//			System.out.print("\t");
//			System.out.print(cc[counter[2]]);
//			System.out.println();
//
//			handle();
//		}
		List<String> cartesian = cartesian(xyz);
		for (String s : cartesian){
			System.out.println(s);
		}
	}

	public static void handle() {
		counter[counterIndex]++;
		if (counter[counterIndex] >= xyz[counterIndex].length) {
			counter[counterIndex] = 0;
			counterIndex--;
			if (counterIndex >= 0) {
				handle();
			}
			counterIndex = xyz.length - 1;
		}
	}

}
