package org.onesentence.onesentence.domain.chat.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.checkerframework.checker.units.qual.C;
import org.onesentence.onesentence.domain.chat.dto.ChatMessageDto;

@Getter
@Builder
@Entity
@AllArgsConstructor
@NoArgsConstructor
public class Chat {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "chat_id")
	private Long id;

	@Column
	private String message;

	public Chat(ChatMessageDto chatMessageDto) {
		this.message = chatMessageDto.getMessage();
	}


}
