package com.maomao.es;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author maohongqi
 * @Date 2020/2/27 14:22
 * @Version 1.0
 **/
public class OperationES {

    public Map<String, Object> addTopic(String data) {
        Map<String, Object> map = new HashMap<>();
        //连接ES
        TransportClient transportClient = ESClientConnectionUtil.getESClientConnection();
        JSONObject json = JSONObject.parseObject(data);//后台传过来的对象数据转换成json格式
        try {
            //index 索引名称（相当于数据库） type :类型（相当于数据库中的表）
            IndexResponse response = transportClient.prepareIndex("knowledge", "knowledge_theme").setSource(json, XContentType.JSON).get();
            if (null != response.getId()) {
                map.put("code", 200);
                return map;
            }
        } catch (Exception e) {
            e.printStackTrace();
            map.put("code", 500);
            return map;
        }
        return null;
    }

    public void searchES() {
        //连接ES
        TransportClient transportClient = ESClientConnectionUtil.getESClientConnection();
        //参数：索引名，类型（type） id
        GetResponse response = transportClient.prepareGet("knowledge", "knowledge_theme", "1")
                .setOperationThreaded(false)    // 线程安全
                .get();
        JSONObject obj = JSONObject.parseObject(response.getSourceAsString());//将json字符串转换为json对象
        String data = obj.toString();

        String codes = response.getId();//获取_id
    }

    public void searchESmohu() {
        SearchResponse searchResponse = null;
        //连接elasticsearch
        TransportClient transportClient = ESClientConnectionUtil.getESClientConnection();
        String name = "mohu";
        searchResponse = transportClient.prepareSearch()
                .setIndices("knowledge")
                .setTypes("knowledge_theme")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setScroll(TimeValue.timeValueMinutes(30)) //游标维持时间
                .setSize(2 * 5)//实际返回的数量为10*index的主分片数
                .setQuery(QueryBuilders.wildcardQuery("name", ("*" + name + "*").toLowerCase()))  //查询的字段名及值
                .execute()
                .actionGet();

    }

    public void updateES() {
        //连接ES
        TransportClient transportClient = ESClientConnectionUtil.getESClientConnection();

        //knowledgeTopic为修改数据的对象
        //修改状态后的对象转换成json数据
        String knowledgeTopic = "";
        JSONObject fromObject = JSONObject.parseObject(knowledgeTopic);
        //参数：索引名，类型（type） id（指的是_id） 要修改的json数据：fromObject
        UpdateResponse updateResponse = transportClient.prepareUpdate("knowledge", "knowledge_theme", "1")
                .setDoc(fromObject).get();
    }


}
