package com.monitora.dc.model;

public class MetricaBruta {

    public String idServidor;
    public String timestamp;
    public String cpu;
    public String totalRam;
    public String ramUsada;
    public String ramPercent;
    public String ramQuente;
    public String ramFria;
    public String discoPercent;
    public String bytesEnviados;
    public String bytesRecebidos;
    public String usoRedeMb;
    public String latencia;
    public String pacotesEnviados;
    public String pacotesRecebidos;
    public String pacotesPerdidos;
    public String qtdProcessos;
    public String uptimeSegundos;

    public MetricaBruta() {}

    public MetricaBruta(String[] col) {
        this.idServidor = col[0];
        this.timestamp = col[1];

        this.cpu = col[2];
        this.totalRam = col[3];
        this.ramUsada = col[4];
        this.ramPercent = col[5];

        this.ramQuente = col[6];
        this.ramFria = col[7];

        this.discoPercent = col[8];

        this.bytesEnviados = col[9];
        this.bytesRecebidos = col[10];
        this.usoRedeMb = col[11];

        this.latencia = col[12];

        this.pacotesEnviados = col[13];
        this.pacotesRecebidos = col[14];
        this.pacotesPerdidos = col[15];

        this.qtdProcessos = col[16];
        this.uptimeSegundos = col[17];
    }
}
