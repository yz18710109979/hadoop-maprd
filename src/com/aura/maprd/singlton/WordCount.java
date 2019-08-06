package com.aura.maprd.singlton;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * javase实现
 * @author yz
 * @date 2019/8/3 11:01
 */
public class WordCount {
    @SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
    	Map<String, Integer> map01 = readOnFile("D:\\doc\\wc\\1.txt");
    	System.out.println(map01);
    	Map<String, Integer> resmap = mergeAllFile(map01);
    	System.out.println(resmap);
    }
    //读取每个文件的信息并统计
    public static Map<String,Integer> readOnFile(String path) throws Exception{
        Map<String,Integer> map = new HashMap<>();
        BufferedReader br = new BufferedReader(new FileReader(path));
        String line = null;
        while((line = br.readLine()) != null) {
        	String[] words = line.split("\t");
        	for (String word : words) {
				if(!map.containsKey(word)) {
					map.put(word, 1);
				}else {
					int newvalue = map.get(word)+1;
					map.put(word, newvalue);
				}
			}
        }
        br.close();
        return map;
    }
    //汇总
    @SuppressWarnings("unchecked")
	public static Map<String,Integer> mergeAllFile(Map<String,Integer>...maps){
        Map<String,Integer> resmap = new HashMap<>();
        for (Map<String, Integer> map : maps) {
			//keys
        	Set<String> keys = map.keySet();
        	for (String key : keys) {
				if(!resmap.containsKey(key)) {
					resmap.put(key, map.get(key));
				}else {
					int newvalue = resmap.get(key) + map.get(key);
					resmap.put(key, newvalue);
				}
			}
		}
        return resmap;
    }
}

