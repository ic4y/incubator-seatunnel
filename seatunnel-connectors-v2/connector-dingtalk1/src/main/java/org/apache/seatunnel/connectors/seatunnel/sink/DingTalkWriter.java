package org.apache.seatunnel.connectors.seatunnel.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.request.OapiRobotSendRequest;
import com.dingtalk.api.response.OapiRobotSendResponse;
import com.taobao.api.ApiException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.io.Serializable;
import java.net.URLEncoder;
import java.util.Base64;

/**
 * @Author: Liuli
 * @Date: 2022/7/24 15:02
 */
public class DingTalkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private RobotClient robotClient;

    public DingTalkWriter(String url, String secret){
        this.robotClient = new RobotClient(url, secret);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        robotClient.send(element.toString());
    }

    @Override
    public void close() throws IOException {

    }

    private static class RobotClient implements Serializable {
        private String url;

        private String secret;

        private DefaultDingTalkClient client;

        public RobotClient(String url, String secret) {
            this.url = url;
            this.secret = secret;
        }

        public OapiRobotSendResponse send(String message) throws IOException {
            if (null == client) {
                client = new DefaultDingTalkClient(getUrl());
            }
            OapiRobotSendRequest request = new OapiRobotSendRequest();
            request.setMsgtype("text");
            OapiRobotSendRequest.Text text = new OapiRobotSendRequest.Text();
            text.setContent(message);
            request.setText(text);
            try {
                return this.client.execute(request);
            } catch (ApiException e) {
                throw new IOException(e);
            }
        }

        public String getUrl() {
            Long timestamp = System.currentTimeMillis();
            String sign = getSign(timestamp);
            return url + "&timestamp=" + timestamp + "&sign=" + sign;
        }

        public String getSign(Long timestamp) {
            try {
                String stringToSign = timestamp + "\n" + secret;
                Mac mac = Mac.getInstance("HmacSHA256");
                mac.init(new SecretKeySpec(secret.getBytes("UTF-8"), "HmacSHA256"));
                byte[] signData = mac.doFinal(stringToSign.getBytes("UTF-8"));
                return URLEncoder.encode(Base64.getEncoder().encodeToString(signData), "UTF-8");
            } catch (Exception e) {
                return null;
            }
        }
    }


}
