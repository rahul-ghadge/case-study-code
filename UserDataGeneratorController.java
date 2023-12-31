package com.arya.hmac.auth.utils;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
@RequestMapping("/data")
public class UserDataGeneratorController {

    @GetMapping("/{noOfRecords}")
    public ResponseEntity<?> findById(@PathVariable int noOfRecords) throws IOException {
        return ResponseEntity.ok(FakeDataGenerator.getData(noOfRecords));

    }
}
