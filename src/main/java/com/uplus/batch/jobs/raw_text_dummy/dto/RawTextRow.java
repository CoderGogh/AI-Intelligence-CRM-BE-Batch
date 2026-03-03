package com.uplus.batch.jobs.raw_text_dummy.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class RawTextRow {

    private final Long consultId;
    private final String rawTextJson;
}
