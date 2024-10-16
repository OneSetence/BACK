package org.onesentence.onesentence.domain.text.controller;

import com.google.firebase.messaging.FirebaseMessagingException;
import java.io.IOException;
import java.net.URI;
import lombok.RequiredArgsConstructor;
import org.onesentence.onesentence.domain.text.dto.TextRequest;
import org.onesentence.onesentence.domain.text.service.TextService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/texts")
public class TextController {

	private final TextService textService;

	@PostMapping
	public ResponseEntity<String> createText(@RequestBody TextRequest request,
		@RequestAttribute("userId") Long userId)
		throws IOException, FirebaseMessagingException {

		Long todoId = textService.createTodoByOneSentence(request, userId);

		return ResponseEntity.created(URI.create("/api/v1/texts/" + todoId)).build();
	}
}