package com.monitora.dc.model;

public class MetricaTratada {
    public String idServidor;
    public String timestamp;
    public double cpuPercent;
    public double ramPercent;
    public double discoPercent;
    public double redeMb;
    public double latenciaMs;
    public int processos;
    public double saude;
    public boolean up;
    public String cor;

    public MetricaTratada() {}
}
