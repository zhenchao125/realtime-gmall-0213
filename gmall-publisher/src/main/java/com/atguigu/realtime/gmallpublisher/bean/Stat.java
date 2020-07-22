package com.atguigu.realtime.gmallpublisher.bean;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @Author lzc
 * @Date 2020/7/22 11:10
 */
public class Stat {
    private String title;
    private List<Option> options = new ArrayList<>();

    public Stat() {
    }

    public Stat(String title) {
        this.title = title;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Option> getOptions() {
        return options;
    }

    public void addOption(Option opt, Option... others) {
        options.add(opt);
        Collections.addAll(options, others);
    }

}
