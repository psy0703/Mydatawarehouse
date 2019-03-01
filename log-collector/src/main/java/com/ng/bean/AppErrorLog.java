package com.ng.bean;

/**
 * 错误日志
 */
public class AppErrorLog {
    //错误摘要
    private String errorBrif;
    //错误详情
    private String errorDetail;

    public String getErrorBrif() {
        return errorBrif;
    }

    public void setErrorBrif(String errorBrif) {
        this.errorBrif = errorBrif;
    }

    public String getErrorDetail() {
        return errorDetail;
    }

    public void setErrorDetail(String errorDetail) {
        this.errorDetail = errorDetail;
    }
}
