package etl;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class Carregar {

    private static final String TRUSTED_BUCKET = "monitora-trusted";
    private final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

    public void salvarEmTrusted(List<String[]> dados, String keyOrigem) {
        StringBuilder sb = new StringBuilder();
        for (String[] linha : dados) {
            sb.append(String.join(";", linha)).append("\n");
        }

        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        ByteArrayInputStream input = new ByteArrayInputStream(bytes);

        String trustedKey = keyOrigem;

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(bytes.length);
        metadata.setContentType("text/csv");

        s3.putObject(TRUSTED_BUCKET, trustedKey, input, metadata);

        System.out.println("Arquivo CSV salvo em s3://" + TRUSTED_BUCKET + "/" + trustedKey);
    }
}
