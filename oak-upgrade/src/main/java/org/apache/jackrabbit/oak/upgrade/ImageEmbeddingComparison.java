package org.apache.jackrabbit.oak.upgrade;

import ai.djl.Model;
import ai.djl.inference.Predictor;
import ai.djl.modality.Classifications;
import ai.djl.modality.cv.Image;
import ai.djl.modality.cv.ImageFactory;
import ai.djl.modality.cv.transform.Normalize;
import ai.djl.modality.cv.transform.Resize;
import ai.djl.modality.cv.transform.ToTensor;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelZoo;
import ai.djl.translate.Batchifier;
import ai.djl.translate.TranslateException;
import ai.djl.translate.Translator;
import ai.djl.translate.TranslatorContext;

import java.io.FileInputStream;
import java.io.InputStream;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

public class ImageEmbeddingComparison {

  private static final Logger log = Logger.getLogger(ImageEmbeddingComparison.class.getName());

  public static void compareImages(String image1Path, String image2Path) {
    // API URL
    String apiUrl = "http://127.0.0.1:5000/compare";

    //image1Path = "/Users/bwadhwa/Downloads/twishaDoc/surbheeadhar.jpg";
    //image2Path = "/Users/bwadhwa/Downloads/twishaDoc/surbheeadhar.jpg";
    // Construct the JSON payload
    String jsonPayload = String.format(
      "{\"image1_path\": \"%s\", \"image2_path\": \"%s\"}",
      image1Path, image2Path
    );

    HttpURLConnection connection = null;
    try {
      // Create the URL object
      URL url = new URL(apiUrl);

      // Open the connection
      connection = (HttpURLConnection) url.openConnection();

      // Set the request method to POST
      connection.setRequestMethod("POST");

      // Set the request headers
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setDoOutput(true);

      // Write the JSON payload to the output stream
      try (OutputStream os = connection.getOutputStream()) {
        byte[] input = jsonPayload.getBytes(StandardCharsets.UTF_8);
        os.write(input, 0, input.length);
      }

      log.info("API Request: " + jsonPayload);
      // Get the response code
      int responseCode = connection.getResponseCode();
      log.info("Response Code: " + responseCode);

      // Read the response from the input stream
      try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
        String inputLine;
        StringBuilder response = new StringBuilder();
        while ((inputLine = in.readLine()) != null) {
          response.append(inputLine);
        }

        // Log the response (for debugging)
        log.info("API Response: " + response.toString());

        // Parse the similarity score from the JSON response
        if (response.toString().contains("similarity_score")) {
          String similarityScore = response.toString().split(":")[1].replace("}", "").replace("\"", "").trim();
          log.info("Similarity Score: " + similarityScore);
        } else {
          log.warning("API response did not contain similarity score.");
        }

      }

    } catch (IOException e) {
      log.severe("Error during API call: " + e.getMessage());
      e.printStackTrace();
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }
}
